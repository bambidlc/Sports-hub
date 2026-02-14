import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SESSION_PATH = DATA_DIR / "session.json"
EVENTS_PATH = DATA_DIR / "events.jsonl"


def load_session() -> dict:
    if SESSION_PATH.exists():
        return json.loads(SESSION_PATH.read_text(encoding="utf-8"))
    return {}


def save_session(session: dict) -> None:
    SESSION_PATH.write_text(json.dumps(session, indent=2), encoding="utf-8")


def load_events() -> list:
    if not EVENTS_PATH.exists():
        return []
    events = []
    with EVENTS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def append_event(event: dict) -> None:
    line = json.dumps(event, separators=(",", ":"))
    with EVENTS_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def rewrite_events(events: list) -> None:
    with EVENTS_PATH.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")


def compute_stats(events: list, session: dict) -> dict:
    home_team = session.get("home_team_name", "Home")
    away_team = session.get("away_team_name", "Away")
    
    home_score = 0
    away_score = 0
    home_fouls = 0
    away_fouls = 0
    
    player_stats = {}
    
    for event in events:
        team = event.get("team", "")
        event_type = event.get("event_type", "")
        player_name = event.get("player_name", "")
        
        if player_name not in player_stats:
            player_stats[player_name] = {
                "name": player_name,
                "team": team,
                "points": 0,
                "fouls": 0,
                "assists": 0,
                "rebounds": 0,
            }
        
        if event_type == "score":
            points = int(event.get("points", 0))
            player_stats[player_name]["points"] += points
            if team == "home":
                home_score += points
            else:
                away_score += points
        elif event_type == "foul":
            player_stats[player_name]["fouls"] += 1
            if team == "home":
                home_fouls += 1
            else:
                away_fouls += 1
        elif event_type == "assist":
            player_stats[player_name]["assists"] += 1
        elif event_type == "rebound":
            player_stats[player_name]["rebounds"] += 1
    
    return {
        "home_score": home_score,
        "away_score": away_score,
        "home_fouls": home_fouls,
        "away_fouls": away_fouls,
        "home_team_name": home_team,
        "away_team_name": away_team,
        "player_stats": player_stats,
    }


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data: dict, status: int = 200) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_text(self, text: str, status: int = 200, content_type: str = "text/plain") -> None:
        data = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        
        if parsed.path == "/api/session":
            self._send_json({"session": load_session()})
            return
        
        if parsed.path == "/api/stats":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            events = load_events()
            self._send_json(compute_stats(events, session))
            return
        
        if parsed.path == "/api/events":
            events = load_events()
            self._send_json({"events": events})
            return
        
        if parsed.path in ["/embed", "/embed/"]:
            embed_path = STATIC_DIR / "iframe.html"
            if embed_path.exists():
                self._send_text(embed_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Embed not found</body></html>", content_type="text/html")
            return
        
        if parsed.path in ["/stream-desk", "/stream-desk/"]:
            stream_path = STATIC_DIR / "stream_desk.html"
            if stream_path.exists():
                self._send_text(stream_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Stream desk not found</body></html>", content_type="text/html")
            return
        
        if parsed.path in ["/overlay", "/overlay/"]:
            overlay_path = STATIC_DIR / "overlay_new.html"
            if overlay_path.exists():
                self._send_text(overlay_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Overlay not found</body></html>", content_type="text/html")
            return
        
        if parsed.path in ["/scoreboard", "/scoreboard/"]:
            scoreboard_path = STATIC_DIR / "scoreboard.html"
            if scoreboard_path.exists():
                self._send_text(scoreboard_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Scoreboard not found</body></html>", content_type="text/html")
            return
        
        if parsed.path in ["/stream-desk", "/stream-desk/"]:
            stream_path = STATIC_DIR / "stream_desk_new.html"
            if stream_path.exists():
                self._send_text(stream_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Stream desk not found</body></html>", content_type="text/html")
            return
        
        if parsed.path == "/":
            index_path = STATIC_DIR / "sports_hub_index.html"
            if index_path.exists():
                self._send_text(index_path.read_text(encoding="utf-8"), content_type="text/html")
            else:
                self._send_text("<html><body>Index not found</body></html>", content_type="text/html")
            return
        
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        
        if parsed.path == "/api/start":
            data = self._read_json()
            sport = data.get("sport", "basketball")
            home_team = data.get("home_team_name", "Home")
            away_team = data.get("away_team_name", "Away")
            home_color = data.get("home_color", "#3b82f6")
            away_color = data.get("away_color", "#ef4444")
            
            session = {
                "sport": sport,
                "home_team_name": home_team,
                "away_team_name": away_team,
                "home_color": home_color,
                "away_color": away_color,
                "period": 1,
                "clock_running": False,
                "clock_seconds": 720,  # 12 minutes default
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "finalized_at": None,
            }
            save_session(session)
            EVENTS_PATH.write_text("", encoding="utf-8")
            self._send_json({"session": session})
            return
        
        if parsed.path == "/api/session":
            data = self._read_json()
            session = load_session()
            session.update(data)
            save_session(session)
            self._send_json({"session": session})
            return
        
        if parsed.path == "/api/event":
            data = self._read_json()
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            
            event = {
                "id": int(time.time() * 1000),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                **data
            }
            append_event(event)
            events = load_events()
            self._send_json(compute_stats(events, session))
            return
        
        if parsed.path == "/api/event/delete":
            data = self._read_json()
            event_id = data.get("id")
            if event_id is None:
                self._send_json({"error": "id required"}, status=400)
                return
            events = load_events()
            events = [e for e in events if e.get("id") != event_id]
            rewrite_events(events)
            session = load_session()
            self._send_json(compute_stats(events, session))
            return
        
        if parsed.path == "/api/event/clear":
            EVENTS_PATH.write_text("", encoding="utf-8")
            session = load_session()
            self._send_json(compute_stats([], session))
            return
        
        if parsed.path == "/api/finalize":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            session["finalized_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save_session(session)
            self._send_json({"ok": True})
            return
        
        self.send_response(404)
        self.end_headers()


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Sports Hub running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
