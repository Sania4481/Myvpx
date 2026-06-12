import asyncio
import json
import os
import queue
import threading
import time
import uuid
import requests as req_lib

from flask import Flask, Response, jsonify, request, send_file

from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, LikeEvent, GiftEvent, DisconnectEvent, ConnectEvent

try:
    from TikTokLive.client.web.web_defaults import WebDefaults
    WebDefaults.tiktok_sign_url = "https://sign.tiktoklive.app/"
except ImportError:
    pass

app = Flask(__name__)

# ══════════════════════════════════════════
# SETTINGS PERSISTENCE
# ══════════════════════════════════════════
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
DEFAULT_USERNAME = "@ganji_live_8"

_settings_lock = threading.Lock()


def _load_settings() -> dict:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "username" in data:
                return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {"username": DEFAULT_USERNAME}


def _save_settings(data: dict):
    with _settings_lock:
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            print(f"[Settings] Save error: {e}")


def _normalize_username(raw: str) -> str:
    raw = raw.strip().lstrip("@").strip()
    if not raw:
        return ""
    return "@" + raw


# Startup mein saved username load karo
_current_settings  = _load_settings()
TIKTOK_USERNAME    = _current_settings["username"]

# ══════════════════════════════════════════
# GLOBAL STATE
# ══════════════════════════════════════════
state = {
    "streamer": TIKTOK_USERNAME.replace("@", ""),
    "is_live": False,
    "total_likes": 0,
    "total_coins": 0,
    "likes": [],
    "gifts": [],
    "notifications": [],
    "team_comments": [],
    "battle": {
        "active": False,
        "duration": 120,
        "remaining": 0,
        "end_time": 0,
        "score_a": 0,
        "score_b": 0,
        "count_a": 0,
        "count_b": 0,
        "winner": None,
        "all_a": [],
        "all_b": [],
        "top_a": [],
        "top_b": [],
        "players": {}
    }
}

internal_set_a:       set  = set()
internal_set_b:       set  = set()
join_order_a:         list = []
join_order_b:         list = []
global_likes_tracker: dict = {}
global_gifts_tracker: dict = {}
user_avatars:         dict = {}

_likes_dirty  = False
_gifts_dirty  = False
_battle_dirty = False

_sse_clients: list = []
_sse_lock = threading.Lock()

# ── Username change signalling ──
# Jab bhi username change ho, ye Event set hoti hai.
# run_tiktok_client() loop isko check kar ke turant reconnect karta hai.
# Har iteration ke shuru mein clear() hoti hai — koi race condition nahi.
_username_change_event = threading.Event()


def push_state():
    payload = "data: " + json.dumps(state, default=str) + "\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


# ══════════════════════════════════════════
# BACKGROUND WORKER
# ══════════════════════════════════════════
def background_worker():
    global _likes_dirty, _gifts_dirty, _battle_dirty
    last_push = 0.0

    while True:
        time.sleep(0.25)
        changed = False

        if _likes_dirty:
            state["likes"] = sorted(
                [{"nickname": k, "likes": v, "avatar": user_avatars.get(k, "")}
                 for k, v in global_likes_tracker.items()],
                key=lambda x: x["likes"], reverse=True
            )[:10]
            _likes_dirty = False
            changed = True

        if _gifts_dirty:
            state["gifts"] = sorted(
                [{"nickname": k, "coins": v, "avatar": user_avatars.get(k, "")}
                 for k, v in global_gifts_tracker.items()],
                key=lambda x: x["coins"], reverse=True
            )[:10]
            _gifts_dirty = False
            changed = True

        if state["battle"]["active"]:
            end_time  = state["battle"]["end_time"]
            now       = time.time()
            remaining = int(end_time - now)
            if end_time > 0 and remaining <= 0:
                state["battle"]["active"]    = False
                state["battle"]["remaining"] = 0
                sa = state["battle"]["score_a"]
                sb = state["battle"]["score_b"]
                state["battle"]["winner"] = "A" if sa > sb else ("B" if sb > sa else "DRAW")
                changed = True
            elif remaining != state["battle"]["remaining"]:
                state["battle"]["remaining"] = remaining
                changed = True

        if _battle_dirty:
            _update_top_lists()
            _battle_dirty = False
            changed = True

        now = time.time()
        if changed or (now - last_push) >= 2.0:
            push_state()
            last_push = now


threading.Thread(target=background_worker, daemon=True).start()


# ══════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════
def extract_avatar_url(user) -> str:
    for attr in ("avatar_thumb", "avatar_medium", "avatar_large", "avatar_jpg"):
        try:
            img = getattr(user, attr, None)
            if not img:
                continue
            # TikTokLive v6.x → m_urls,  v7.x → url_list
            url_list = (
                getattr(img, "m_urls", None)
                or getattr(img, "url_list", None)
                or getattr(img, "urls", None)
            )
            # Kuch versions mein direct string URL hoti hai
            if not url_list and isinstance(img, str) and img.startswith("http"):
                return img
            if not url_list:
                continue
            for u in url_list:
                if u and ("muscdn.com" in u or "tiktokcdn-us.com" in u):
                    return u
            for u in url_list:
                if u:
                    return u
        except Exception as e:
            print(f"[Avatar] Error reading {attr}: {e}")
    return ""


def remember_avatar(nickname: str, user) -> str:
    url = extract_avatar_url(user)
    if url:
        user_avatars[nickname] = url
    return user_avatars.get(nickname, "")


def get_user_team(nickname: str):
    if nickname in internal_set_a:
        return "A"
    if nickname in internal_set_b:
        return "B"
    return None


def _update_top_lists():
    players      = state["battle"]["players"]
    battle_active = state["battle"]["active"]
    team_a, team_b = [], []
    for nick, data in players.items():
        team  = get_user_team(nick)
        entry = {"nickname": nick, "likes": data["likes"],
                 "coins": data["coins"], "avatar": user_avatars.get(nick, "")}
        if team == "A":
            team_a.append(entry)
        elif team == "B":
            team_b.append(entry)

    if battle_active:
        key_fn = lambda x: x["likes"] + x["coins"] * 50
        state["battle"]["top_a"] = sorted(team_a, key=key_fn, reverse=True)[:5]
        state["battle"]["top_b"] = sorted(team_b, key=key_fn, reverse=True)[:5]
    else:
        def join_order_key_a(x):
            try:   return join_order_a.index(x["nickname"])
            except ValueError: return 9999
        def join_order_key_b(x):
            try:   return join_order_b.index(x["nickname"])
            except ValueError: return 9999
        state["battle"]["top_a"] = sorted(team_a, key=join_order_key_a)[:5]
        state["battle"]["top_b"] = sorted(team_b, key=join_order_key_b)[:5]


def _sync_team_counts():
    state["battle"]["all_a"]   = list(internal_set_a)
    state["battle"]["all_b"]   = list(internal_set_b)
    state["battle"]["count_a"] = len(internal_set_a)
    state["battle"]["count_b"] = len(internal_set_b)


# ══════════════════════════════════════════
# TIKTOK EVENT HANDLERS
# ══════════════════════════════════════════
async def on_comment(event: CommentEvent):
    global _battle_dirty
    nick   = event.user.nickname
    avatar = remember_avatar(nick, event.user)
    text   = event.comment.strip()
    text_lower = text.lower()

    team_assigned = None
    if text_lower in ["!a", "a", "team a", "teama"]:
        team_assigned = "A"
    elif text_lower in ["!b", "b", "team b", "teamb"]:
        team_assigned = "B"

    if team_assigned:
        current_team  = get_user_team(nick)
        battle_active = state["battle"]["active"]

        should_assign = False
        if not current_team:
            should_assign = True
        elif current_team != team_assigned and not battle_active:
            should_assign = True

        if should_assign:
            internal_set_a.discard(nick)
            internal_set_b.discard(nick)
            if nick in join_order_a: join_order_a.remove(nick)
            if nick in join_order_b: join_order_b.remove(nick)
            (internal_set_a if team_assigned == "A" else internal_set_b).add(nick)
            (join_order_a if team_assigned == "A" else join_order_b).append(nick)
            _sync_team_counts()

            state["notifications"].append({
                "id": str(uuid.uuid4()),
                "nickname": nick,
                "team": team_assigned,
                "avatar": avatar
            })
            if len(state["notifications"]) > 200:
                state["notifications"] = state["notifications"][-100:]

            state["battle"]["players"].setdefault(nick, {"likes": 0, "coins": 0})
            _battle_dirty = True

        effective_team = team_assigned if should_assign else current_team
        if effective_team:
            state["team_comments"].append({
                "nickname": nick,
                "comment": text,
                "team": effective_team,
                "avatar": avatar
            })
            if len(state["team_comments"]) > 30:
                state["team_comments"].pop(0)
            push_state()


async def on_like(event: LikeEvent):
    global _likes_dirty, _battle_dirty
    nick = event.user.nickname
    remember_avatar(nick, event.user)
    like_count = int(
        getattr(event, "count", None)
        or getattr(event, "likes", None)
        or getattr(event, "like_count", None)
        or 1
    )

    state["total_likes"] += like_count
    global_likes_tracker[nick] = global_likes_tracker.get(nick, 0) + like_count
    _likes_dirty = True

    if state["battle"]["active"]:
        user_team = get_user_team(nick)
        if user_team:
            score_key = "score_a" if user_team == "A" else "score_b"
            state["battle"][score_key] += like_count
            state["battle"]["players"].setdefault(nick, {"likes": 0, "coins": 0})
            state["battle"]["players"][nick]["likes"] += like_count
            _battle_dirty = True


async def on_gift(event: GiftEvent):
    global _gifts_dirty, _battle_dirty
    if event.streaking:
        return
    gift = event.gift
    if gift is None:
        return

    nick = event.user.nickname
    remember_avatar(nick, event.user)
    diamond_count = int(
        getattr(gift, "diamond_count", None)
        or getattr(getattr(gift, "info", None), "diamond_count", None)
        or 1
    )
    repeat = int(getattr(event, "repeat_count", 1) or 1)
    coins  = diamond_count * repeat

    if coins > 0:
        state["total_coins"] += coins
        global_gifts_tracker[nick] = global_gifts_tracker.get(nick, 0) + coins
        _gifts_dirty = True

        if state["battle"]["active"]:
            user_team = get_user_team(nick)
            if user_team:
                score_key = "score_a" if user_team == "A" else "score_b"
                state["battle"][score_key] += coins * 50
                state["battle"]["players"].setdefault(nick, {"likes": 0, "coins": 0})
                state["battle"]["players"][nick]["coins"] += coins
                _battle_dirty = True

        push_state()


async def on_connect(event: ConnectEvent):
    state["is_live"] = True
    push_state()
    print(f"[TikTokLive] ✅ Connected to {TIKTOK_USERNAME} live stream!")


async def on_disconnect(event: DisconnectEvent):
    state["is_live"] = False
    push_state()
    print(f"[TikTokLive] ❌ Disconnected from {TIKTOK_USERNAME}.")


# ══════════════════════════════════════════
# TIKTOK CLIENT LOOP
# ══════════════════════════════════════════
def run_tiktok_client():
    """
    Smart polling loop with clean username-change support:

    1. Har iteration ki shuru mein _username_change_event.clear() karo
       → pehli baar ya dobara set() hone se koi issue nahi
    2. current_username snapshot lo — is iteration ke liye ye fixed hai
    3. connect() block karta hai jab tak stream chal raha ho
    4. Username change hone par:
       a. /api/settings endpoint TIKTOK_USERNAME update karta hai
       b. _username_change_event.set() karta hai
       c. Asyncio loop mein stop_signal Future resolve ho jaata hai
       d. connect() return ho jaata hai, loop restart hota hai — koi crash nahi
    """
    global TIKTOK_USERNAME

    POLL_INTERVAL   = 30   # seconds — offline ho to kitni der baad check karo
    RECONNECT_DELAY = 5    # seconds — unexpected error ke baad delay

    while True:
        # ── Har iteration mein fresh clear ──
        _username_change_event.clear()
        current_username = TIKTOK_USERNAME   # snapshot

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # stop_signal Future — username change hone par resolve hota hai
        stop_signal = loop.create_future()

        def _signal_stop():
            """Thread-safe: main thread se asyncio loop mein Future resolve karo."""
            if not stop_signal.done():
                loop.call_soon_threadsafe(stop_signal.set_result, True)

        # Username change watcher — alag thread se signal bhejta hai
        def _username_watcher():
            _username_change_event.wait()   # block — jab tak change na ho
            _signal_stop()

        watcher_thread = threading.Thread(target=_username_watcher, daemon=True)
        watcher_thread.start()

        try:
            client = TikTokLiveClient(unique_id=current_username)
            client.add_listener(ConnectEvent,    on_connect)
            client.add_listener(CommentEvent,    on_comment)
            client.add_listener(LikeEvent,       on_like)
            client.add_listener(GiftEvent,       on_gift)
            client.add_listener(DisconnectEvent, on_disconnect)

            print(f"[TikTokLive] Checking if {current_username} is live...")

            async def _run():
                """connect() aur stop_signal dono parallel chalao."""
                connect_task = loop.create_task(client.connect())
                # Jo pehle khatam ho — connect ya stop_signal
                done, pending = await asyncio.wait(
                    [connect_task, asyncio.ensure_future(stop_signal)],
                    return_when=asyncio.FIRST_COMPLETED
                )
                # Agar stop_signal pehle aaya to client gracefully disconnect karo
                if stop_signal in done and not connect_task.done():
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    connect_task.cancel()
                    try:
                        await connect_task
                    except (asyncio.CancelledError, Exception):
                        pass
                elif connect_task in done:
                    # Normal stream end — exception check karo
                    exc = connect_task.exception()
                    if exc:
                        raise exc

            loop.run_until_complete(_run())

            # Username change hua tha? → turant restart
            if _username_change_event.is_set():
                print(f"[TikTokLive] Username changed → restarting for {TIKTOK_USERNAME}")
                state["is_live"] = False
                push_state()
                continue

            # Normal stream end
            print(f"[TikTokLive] Stream ended for {current_username}.")
            state["is_live"] = False
            push_state()
            # RECONNECT_DELAY wait — username change hone par early exit
            if _username_change_event.wait(timeout=RECONNECT_DELAY):
                print(f"[TikTokLive] Username changed during wait → restarting")
                continue

        except Exception as e:
            # Username change wajah se interrupt hua
            if _username_change_event.is_set():
                print(f"[TikTokLive] Username changed → restarting for {TIKTOK_USERNAME}")
                state["is_live"] = False
                push_state()
                continue

            err = str(e).lower()
            if any(x in err for x in ["not live", "not_live", "not currently live",
                                       "room_id", "failed to retrieve", "not found",
                                       "offline", "host_not_online"]):
                if state["is_live"]:
                    state["is_live"] = False
                    push_state()
                print(f"[TikTokLive] {current_username} offline. Next check in {POLL_INTERVAL}s...")
                if _username_change_event.wait(timeout=POLL_INTERVAL):
                    print(f"[TikTokLive] Username changed → restarting")
                    continue
            else:
                print(f"[TikTokLive] Unexpected error: {e}")
                state["is_live"] = False
                push_state()
                if _username_change_event.wait(timeout=RECONNECT_DELAY):
                    print(f"[TikTokLive] Username changed → restarting")
                    continue

        finally:
            # Stop signal resolve karo agar abhi tak pending hai
            # (watcher thread block na kare)
            _signal_stop()
            try:
                loop.close()
            except Exception:
                pass


threading.Thread(target=run_tiktok_client, daemon=True).start()


# ══════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════
@app.route("/")
def index():
    return send_file(os.path.join(BASE_DIR, "dashboard.html"))


@app.route("/api/stream")
def stream():
    def event_gen():
        q = queue.Queue(maxsize=20)
        with _sse_lock:
            _sse_clients.append(q)
        try:
            yield "data: " + json.dumps(state, default=str) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            with _sse_lock:
                if q in _sse_clients:
                    _sse_clients.remove(q)

    return Response(
        event_gen(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@app.route("/api/data")
def get_data():
    return jsonify(state)


@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({"username": TIKTOK_USERNAME})


@app.route("/api/settings", methods=["POST"])
def save_settings():
    global TIKTOK_USERNAME
    body         = request.get_json() or {}
    raw          = body.get("username", "")
    new_username = _normalize_username(raw)

    if not new_username:
        return jsonify({"error": "Username khaali nahi ho sakta"}), 400
    if len(new_username) > 50:
        return jsonify({"error": "Username bohat lamba hai"}), 400

    old_username    = TIKTOK_USERNAME
    TIKTOK_USERNAME = new_username
    state["streamer"] = new_username.replace("@", "")
    _save_settings({"username": new_username})

    if old_username != new_username:
        state["is_live"] = False
        push_state()
        # _username_change_event.set() → watcher thread unblock → stop_signal resolve
        # → client.disconnect() → connect() return → loop restart with new username
        _username_change_event.set()
        print(f"[Settings] Username: {old_username} → {new_username}")

    return jsonify({"status": "success", "username": new_username})


@app.route("/api/debug/avatars")
def debug_avatars():
    return jsonify({
        "total": len(user_avatars),
        "avatars": dict(list(user_avatars.items())[:10])
    })


@app.route("/api/avatar")
def proxy_avatar():
    url = request.args.get("url", "").strip()
    allowed = ("tiktokcdn.com", "tiktokcdn-us.com", "musical.ly",
               "p16-sign", "p19-sign", "p77-sign", "p16-amd")
    if not url or not any(d in url for d in allowed):
        return "", 400
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://www.tiktok.com/",
            "Accept":     "image/webp,image/apng,image/*,*/*;q=0.8",
        }
        r = req_lib.get(url, headers=headers, timeout=8, stream=True)
        if r.status_code != 200:
            return "", 404
        ct = r.headers.get("Content-Type", "image/jpeg")
        return Response(
            r.content,
            content_type=ct,
            headers={"Cache-Control": "public, max-age=3600"}
        )
    except Exception as e:
        print(f"[Avatar Proxy] Error: {e}")
        return "", 404


@app.route("/api/battle/start", methods=["POST"])
def start_battle():
    req = request.get_json() or {}
    duration = int(req.get("duration", 120))

    state["battle"]["active"] = False
    new_players = {nick: {"likes": 0, "coins": 0}
                   for nick in (internal_set_a | internal_set_b)}

    state["battle"] = {
        "active":    False,
        "duration":  duration,
        "remaining": duration,
        "end_time":  time.time() + duration,
        "score_a":   0,
        "score_b":   0,
        "count_a":   len(internal_set_a),
        "count_b":   len(internal_set_b),
        "winner":    None,
        "all_a":     list(internal_set_a),
        "all_b":     list(internal_set_b),
        "top_a":     [],
        "top_b":     [],
        "players":   new_players
    }
    state["battle"]["active"] = True
    _update_top_lists()
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/end", methods=["POST"])
def end_battle():
    state["battle"]["active"]    = False
    state["battle"]["remaining"] = 0
    sa, sb = state["battle"]["score_a"], state["battle"]["score_b"]
    state["battle"]["winner"] = "A" if sa > sb else ("B" if sb > sa else "DRAW")
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/reset-scores", methods=["POST"])
def reset_scores():
    state["battle"]["active"]    = False
    state["battle"]["remaining"] = 0
    state["battle"]["end_time"]  = 0
    state["battle"]["score_a"]   = 0
    state["battle"]["score_b"]   = 0
    state["battle"]["winner"]    = None
    state["battle"]["top_a"]     = []
    state["battle"]["top_b"]     = []
    for nick in state["battle"]["players"]:
        state["battle"]["players"][nick] = {"likes": 0, "coins": 0}
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/reset", methods=["POST"])
def reset_all():
    internal_set_a.clear()
    internal_set_b.clear()
    join_order_a.clear()
    join_order_b.clear()
    state["battle"] = {
        "active": False, "duration": 120, "remaining": 0, "end_time": 0,
        "score_a": 0, "score_b": 0, "count_a": 0, "count_b": 0,
        "winner": None, "all_a": [], "all_b": [],
        "top_a": [], "top_b": [], "players": {}
    }
    state["team_comments"]  = []
    state["notifications"]  = []
    push_state()
    return jsonify({"status": "success"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False,
            use_reloader=False, threaded=True)
