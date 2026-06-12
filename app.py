import asyncio
import json
import os
import queue
import threading
import time
import uuid
import urllib.parse
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
# SETTINGS PERSISTENCE (JSON file)
# ══════════════════════════════════════════
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
DEFAULT_USERNAME = "@ganji_live_8"

_settings_lock = threading.Lock()


def _load_settings() -> dict:
    """settings.json se settings load karo, nahi mila to defaults."""
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "username" in data:
                return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {"username": DEFAULT_USERNAME}


def _save_settings(data: dict):
    """settings.json mein save karo."""
    with _settings_lock:
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            print(f"[Settings] Save error: {e}")


def _normalize_username(raw: str) -> str:
    """@ prefix ensure karo, whitespace hatao."""
    raw = raw.strip().lstrip("@").strip()
    if not raw:
        return ""
    return "@" + ra


# Load saved username on startup
_current_settings = _load_settings()
TIKTOK_USERNAME = _current_settings["username"]

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

# internal_set_a/b — team membership (battle ke bahar bhi rehta hai)
internal_set_a: set = set()
internal_set_b: set = set()
# join order tracking — pehle join wala pehle (insertion order preserved in list)
join_order_a: list = []
join_order_b: list = []
global_likes_tracker: dict = {}
global_gifts_tracker: dict = {}
# nickname -> avatar URL (har user ka latest profile picture URL)
user_avatars: dict = {}

_likes_dirty = False
_gifts_dirty = False
_battle_dirty = False

_sse_clients: list = []
_sse_lock = threading.Lock()


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
                [{"nickname": k, "likes": v, "avatar": user_avatars.get(k, "")} for k, v in global_likes_tracker.items()],
                key=lambda x: x["likes"], reverse=True
            )[:10]
            _likes_dirty = False
            changed = True

        if _gifts_dirty:
            state["gifts"] = sorted(
                [{"nickname": k, "coins": v, "avatar": user_avatars.get(k, "")} for k, v in global_gifts_tracker.items()],
                key=lambda x: x["coins"], reverse=True
            )[:10]
            _gifts_dirty = False
            changed = True

        # Battle timer
        if state["battle"]["active"]:
            end_time = state["battle"]["end_time"]
            now = time.time()
            remaining = int(end_time - now)
            if end_time > 0 and remaining <= 0:
                state["battle"]["active"] = False
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
    """
    avatar_thumb / avatar_medium / avatar_large se URL nikaalo.
    TikTokLive v7 mein url_list hai, v6.x mein m_urls hai — dono handle karo.
    """
    for attr in ("avatar_thumb", "avatar_medium", "avatar_large", "avatar_jpg"):
        try:
            img = getattr(user, attr, None)
            if not img:
                continue
            # v6.x (render/PyPI stable) uses m_urls, v7 (beta) uses url_list
            url_list = (
                getattr(img, "m_urls", None)
                or getattr(img, "url_list", None)
                or getattr(img, "urls", None)
            )
            # Some versions may store avatar as a direct string URL
            if not url_list and isinstance(img, str) and img.startswith("http"):
                return img
            if not url_list:
                continue
            # Prefer muscdn / non-sign URLs — ye publicly accessible hote hain
            for u in url_list:
                if u and ("muscdn.com" in u or "tiktokcdn-us.com" in u):
                    return u
            # Fallback: pehla available URL
            for u in url_list:
                if u:
                    return u
        except Exception as e:
            print(f"[Avatar] Error reading {attr}: {e}")
    return ""


def remember_avatar(nickname: str, user) -> str:
    """User ka avatar URL nikal kar cache (user_avatars) mein store karo aur return karo."""
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
    players = state["battle"]["players"]
    battle_active = state["battle"]["active"]
    team_a, team_b = [], []
    for nick, data in players.items():
        team = get_user_team(nick)
        entry = {"nickname": nick, "likes": data["likes"], "coins": data["coins"], "avatar": user_avatars.get(nick, "")}
        if team == "A":
            team_a.append(entry)
        elif team == "B":
            team_b.append(entry)

    if battle_active:
        # Battle chal raha hai — top scorer upar
        key_fn = lambda x: x["likes"] + x["coins"] * 50
        state["battle"]["top_a"] = sorted(team_a, key=key_fn, reverse=True)[:5]
        state["battle"]["top_b"] = sorted(team_b, key=key_fn, reverse=True)[:5]
    else:
        # Battle inactive — join order se sort (pehle join = upar)
        def join_order_key_a(x):
            try: return join_order_a.index(x["nickname"])
            except ValueError: return 9999
        def join_order_key_b(x):
            try: return join_order_b.index(x["nickname"])
            except ValueError: return 9999
        state["battle"]["top_a"] = sorted(team_a, key=join_order_key_a)[:5]
        state["battle"]["top_b"] = sorted(team_b, key=join_order_key_b)[:5]


def _sync_team_counts():
    """internal sets se state mein counts/lists sync karo"""
    state["battle"]["all_a"] = list(internal_set_a)
    state["battle"]["all_b"] = list(internal_set_b)
    state["battle"]["count_a"] = len(internal_set_a)
    state["battle"]["count_b"] = len(internal_set_b)


# ══════════════════════════════════════════
# TIKTOK EVENT HANDLERS
# ══════════════════════════════════════════
async def on_comment(event: CommentEvent):
    global _battle_dirty
    nick = event.user.nickname
    avatar = remember_avatar(nick, event.user)
    text = event.comment.strip()
    text_lower = text.lower()

    team_assigned = None
    if text_lower in ["!a", "a", "team a", "teama"]:
        team_assigned = "A"
    elif text_lower in ["!b", "b", "team b", "teamb"]:
        team_assigned = "B"

    if team_assigned:
        current_team = get_user_team(nick)
        battle_active = state["battle"]["active"]

        # ── TEAM CHANGE RULES ──
        # 1. Pehli baar join: hamesha allow
        # 2. Team change: sirf battle inactive ho tab allow
        # 3. Battle active ho: team lock — change block
        should_assign = False
        if not current_team:
            # Pehli baar join
            should_assign = True
        elif current_team != team_assigned and not battle_active:
            # Team change — battle band ho tab hi allow
            should_assign = True
        # else: battle active ya same team — ignore

        if should_assign:
            # Purani team se remove karo (agar hai)
            internal_set_a.discard(nick)
            internal_set_b.discard(nick)
            # Purani join order se bhi remove karo
            if nick in join_order_a: join_order_a.remove(nick)
            if nick in join_order_b: join_order_b.remove(nick)
            # Nayi team mein add karo
            (internal_set_a if team_assigned == "A" else internal_set_b).add(nick)
            # Join order mein add karo (end mein — latest joiner)
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

            # Players dict mein entry ensure karo (scores touch mat karo)
            state["battle"]["players"].setdefault(nick, {"likes": 0, "coins": 0})
            _battle_dirty = True

        # Comment feed mein daalo (team info ke saath)
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
    coins = diamond_count * repeat

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
    """Sirf tab is_live=True karo jab TikTok confirm kare ke connected hai"""
    state["is_live"] = True
    push_state()
    print(f"[TikTokLive] ✅ Connected to {TIKTOK_USERNAME} live stream!")


async def on_disconnect(event: DisconnectEvent):
    state["is_live"] = False
    push_state()
    print(f"[TikTokLive] ❌ Disconnected from {TIKTOK_USERNAME}.")


_tiktok_reconnect = threading.Event()
_current_client = None
_client_lock = threading.Lock()


def _disconnect_current_client():
    """Agar koi active TikTok client hai to usko force disconnect karo."""
    with _client_lock:
        c = _current_client
    if c is None:
        return
    try:
        # TikTokLive client ka disconnect — connection tod deta hai
        # connect() unblock ho jayega aur loop restart karega
        import asyncio as _aio
        loop = getattr(c, '_ws', None) and getattr(c, '_event_loop', None)
        if hasattr(c, 'disconnect'):
            # Try async disconnect from sync context
            try:
                fut = asyncio.run_coroutine_threadsafe(c.disconnect(), c._event_loop)
                fut.result(timeout=3)
            except Exception:
                pass
        if hasattr(c, 'close'):
            try:
                c.close()
            except Exception:
                pass
    except Exception as e:
        print(f"[TikTokLive] Force disconnect error (safe to ignore): {e}")


def create_client() -> TikTokLiveClient:
    c = TikTokLiveClient(unique_id=TIKTOK_USERNAME)
    c.add_listener(ConnectEvent, on_connect)
    c.add_listener(CommentEvent, on_comment)
    c.add_listener(LikeEvent, on_like)
    c.add_listener(GiftEvent, on_gift)
    c.add_listener(DisconnectEvent, on_disconnect)
    return c


def run_tiktok_client():
    """
    Smart polling loop:
    - Agar user live nahi → 30s wait kar ke dobara check karo (CPU waste nahi)
    - Agar user live ho gaya → connect karo aur events sun-o
    - Agar live khatam ho → disconnect, wapas polling mode
    - Agar username change ho → turant reconnect karo
    """
    global TIKTOK_USERNAME, _current_client
    POLL_INTERVAL   = 30   # seconds — live nahi to kitni der baad check karo
    RECONNECT_DELAY = 10   # seconds — live tha, disconnect hua, reconnect delay

    while True:
        # Check if username changed (reconnect signal)
        _tiktok_reconnect.clear()
        current_username = TIKTOK_USERNAME

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            c = create_client()
            with _client_lock:
                _current_client = c
            print(f"[TikTokLive] Checking if {current_username} is live...")
            loop.run_until_complete(c.connect())
            # connect() returns normally when stream ends
            print(f"[TikTokLive] Stream ended for {current_username}.")
            state["is_live"] = False
            push_state()

            # Username change hua kya? Agar haan to skip delay
            if _tiktok_reconnect.is_set():
                print(f"[TikTokLive] Username changed, reconnecting immediately...")
                continue
            if _tiktok_reconnect.wait(timeout=RECONNECT_DELAY):
                print(f"[TikTokLive] Username changed during wait, reconnecting immediately...")
                continue

        except Exception as e:
            err = str(e).lower()
            # Username change ke wajah se disconnect hua — quickly restart
            if _tiktok_reconnect.is_set():
                print(f"[TikTokLive] Reconnecting for new username: {TIKTOK_USERNAME}...")
                state["is_live"] = False
                push_state()
                continue

            # User not live — expected error, poll quietly
            if any(x in err for x in ["not live", "not_live", "not currently live",
                                       "room_id", "failed to retrieve", "not found",
                                       "offline", "host_not_online"]):
                if state["is_live"]:
                    state["is_live"] = False
                    push_state()
                print(f"[TikTokLive] {current_username} is offline. Next check in {POLL_INTERVAL}s...")
                if _tiktok_reconnect.wait(timeout=POLL_INTERVAL):
                    print(f"[TikTokLive] Username changed, reconnecting immediately...")
                    continue
            else:
                # Unexpected error (network, rate limit, etc.)
                print(f"[TikTokLive] Error: {e}")
                state["is_live"] = False
                push_state()
                if _tiktok_reconnect.wait(timeout=RECONNECT_DELAY):
                    print(f"[TikTokLive] Username changed, reconnecting immediately...")
                    continue
        finally:
            with _client_lock:
                _current_client = None
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


@app.route("/api/debug/avatars")
def debug_avatars():
    """Debug: stored avatar URLs dekho"""
    return jsonify({
        "total": len(user_avatars),
        "avatars": dict(list(user_avatars.items())[:10])
    })


@app.route("/api/avatar")
def proxy_avatar():
    """TikTok CDN avatar URLs ko proxy karo — browser CORS bypass"""
    url = request.args.get("url", "").strip()
    # Sirf TikTok CDN URLs allow karo
    allowed = ("tiktokcdn.com", "tiktokcdn-us.com", "musical.ly",
               "p16-sign", "p19-sign", "p77-sign", "p16-amd")
    if not url or not any(d in url for d in allowed):
        return jsonify({"error": "Invalid or missing avatar URL"}), 400

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.tiktok.com/",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
    }

    try:
        r = req_lib.get(url, headers=headers, timeout=8, stream=True)
    except req_lib.exceptions.ConnectTimeout:
        print(f"[Avatar Proxy] Connect timeout: {url}")
        return jsonify({"error": "TikTok CDN connect timeout"}), 504
    except req_lib.exceptions.ReadTimeout:
        print(f"[Avatar Proxy] Read timeout: {url}")
        return jsonify({"error": "TikTok CDN read timeout"}), 504
    except req_lib.exceptions.ConnectionError as e:
        print(f"[Avatar Proxy] Connection error: {e}")
        return jsonify({"error": "TikTok CDN unreachable"}), 502
    except req_lib.exceptions.TooManyRedirects:
        print(f"[Avatar Proxy] Too many redirects: {url}")
        return jsonify({"error": "TikTok CDN redirect loop"}), 502
    except req_lib.exceptions.RequestException as e:
        print(f"[Avatar Proxy] Request failed: {e}")
        return jsonify({"error": "Failed to fetch avatar"}), 502

    if r.status_code == 403:
        print(f"[Avatar Proxy] CDN returned 403 (signed URL expired): {url}")
        return jsonify({"error": "Avatar URL expired"}), 410
    if r.status_code == 404:
        return jsonify({"error": "Avatar not found on CDN"}), 404
    if r.status_code >= 500:
        print(f"[Avatar Proxy] CDN server error {r.status_code}: {url}")
        return jsonify({"error": "TikTok CDN server error"}), 502
    if r.status_code != 200:
        print(f"[Avatar Proxy] Unexpected status {r.status_code}: {url}")
        return jsonify({"error": f"CDN returned {r.status_code}"}), 502

    ct = r.headers.get("Content-Type", "image/jpeg")
    if not ct.startswith("image/"):
        print(f"[Avatar Proxy] Non-image Content-Type: {ct}")
        return jsonify({"error": "CDN returned non-image content"}), 502

    return Response(
        r.content,
        content_type=ct,
        headers={"Cache-Control": "public, max-age=3600"}
    )


@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({"username": TIKTOK_USERNAME})


@app.route("/api/settings", methods=["POST"])
def save_settings():
    global TIKTOK_USERNAME
    body = request.get_json() or {}
    raw_username = body.get("username", "")
    new_username = _normalize_username(raw_username)

    if not new_username:
        return jsonify({"error": "Username khaali nahi ho sakta"}), 400

    if len(new_username) > 50:
        return jsonify({"error": "Username bohat lamba hai"}), 400

    old_username = TIKTOK_USERNAME
    TIKTOK_USERNAME = new_username
    state["streamer"] = new_username.replace("@", "")

    # File mein persist karo
    _save_settings({"username": new_username})

    # Agar username actually change hua to TikTok client reconnect karo
    if old_username != new_username:
        state["is_live"] = False
        push_state()
        # Pehle signal set karo, phir active client ko force disconnect karo
        _tiktok_reconnect.set()
        _disconnect_current_client()
        print(f"[Settings] Username changed: {old_username} → {new_username}")

    return jsonify({"status": "success", "username": new_username})


@app.route("/api/battle/start", methods=["POST"])
def start_battle():
    req = request.get_json() or {}
    duration = int(req.get("duration", 120))

    # ── Step 1: active=False — background worker ko rok do ──
    state["battle"]["active"] = False

    # ── Step 2: Completely fresh battle state ──
    # Players dict bhi nayi — purane scores bilkul nahi rahenge
    # Teams (internal_set_a/b) rehti hain — log dobara join nahi karenge
    new_players = {nick: {"likes": 0, "coins": 0}
                   for nick in (internal_set_a | internal_set_b)}

    state["battle"] = {
        "active": False,           # Step 3 mein True karenge
        "duration": duration,
        "remaining": duration,
        "end_time": time.time() + duration,
        "score_a": 0,
        "score_b": 0,
        "count_a": len(internal_set_a),
        "count_b": len(internal_set_b),
        "winner": None,
        "all_a": list(internal_set_a),
        "all_b": list(internal_set_b),
        "top_a": [],
        "top_b": [],
        "players": new_players
    }

    # ── Step 3: Ab active=True — sahi end_time set hone ke baad ──
    state["battle"]["active"] = True

    # top_a/top_b turant update karo — background worker ka wait nahi
    _update_top_lists()
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/end", methods=["POST"])
def end_battle():
    state["battle"]["active"] = False
    state["battle"]["remaining"] = 0
    sa, sb = state["battle"]["score_a"], state["battle"]["score_b"]
    state["battle"]["winner"] = "A" if sa > sb else ("B" if sb > sa else "DRAW")
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/reset-scores", methods=["POST"])
def reset_scores():
    # Scores zero karo, teams rehne do
    state["battle"]["active"] = False
    state["battle"]["remaining"] = 0
    state["battle"]["end_time"] = 0
    state["battle"]["score_a"] = 0
    state["battle"]["score_b"] = 0
    state["battle"]["winner"] = None
    state["battle"]["top_a"] = []
    state["battle"]["top_b"] = []
    for nick in state["battle"]["players"]:
        state["battle"]["players"][nick] = {"likes": 0, "coins": 0}
    push_state()
    return jsonify({"status": "success"})


@app.route("/api/battle/reset", methods=["POST"])
def reset_all():
    # Sab kuch clear — teams bhi
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
    state["team_comments"] = []
    state["notifications"] = []
    push_state()
    return jsonify({"status": "success"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False,
            use_reloader=False, threaded=True)
