import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import basketball_stats as bs

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MATCHES_CSV = BASE_DIR / "Matches Name.csv"
PLAYERS_CSV = BASE_DIR / "Player (x_player).csv"
GAME_CSV = BASE_DIR / "Game (x_game).csv"

SESSION_PATH = DATA_DIR / "session.json"
COUNTER_PATH = DATA_DIR / "event_counter.json"


def load_session() -> dict:
    if SESSION_PATH.exists():
        return json.loads(SESSION_PATH.read_text(encoding="utf-8"))
    return {}


def save_session(session: dict) -> None:
    SESSION_PATH.write_text(json.dumps(session, indent=2), encoding="utf-8")


def load_counter() -> int:
    if COUNTER_PATH.exists():
        data = json.loads(COUNTER_PATH.read_text(encoding="utf-8"))
        return int(data.get("next", 1))
    return 1


def save_counter(value: int) -> None:
    COUNTER_PATH.write_text(json.dumps({"next": value}, indent=2), encoding="utf-8")


def load_matches() -> list:
    _fmt, rows = bs.read_csv_with_format(MATCHES_CSV)
    headers = rows[0]
    id_idx = bs.find_header_index(headers, bs.MATCH_COLUMNS["match_id"])
    name_idx = bs.find_header_index(headers, bs.MATCH_COLUMNS["match_name"])
    if id_idx is None or name_idx is None:
        raise ValueError("Matches Name.csv is missing match id or match name column.")
    matches = []
    for row in rows[1:]:
        matches.append({"id": row[id_idx], "name": row[name_idx]})
    return matches


def build_session(match_name: str) -> dict:
    _fmt, matches_rows = bs.read_csv_with_format(MATCHES_CSV)
    _fmt, players_rows = bs.read_csv_with_format(PLAYERS_CSV)
    fmt, game_rows = bs.read_csv_with_format(GAME_CSV)

    match_id = bs.resolve_match_id(matches_rows, match_name)
    row_indices, home_team_id, away_team_id, parent_row_idx = bs.resolve_game_context(
        game_rows, match_id, match_name
    )
    headers = game_rows[0]
    home_name_idx = bs.find_header_index(headers, bs.LOGICAL_COLUMNS["home_team_name"])
    away_name_idx = bs.find_header_index(headers, bs.LOGICAL_COLUMNS["away_team_name"])
    home_team_name = ""
    away_team_name = ""
    if home_name_idx is not None:
        home_team_name = game_rows[parent_row_idx][home_name_idx]
    if away_name_idx is not None:
        away_team_name = game_rows[parent_row_idx][away_name_idx]
    roster = bs.build_roster(players_rows)
    added = bs.expand_game_rows(
        game_rows, game_rows[0], roster, row_indices, home_team_id, away_team_id
    )
    if added:
        bs.write_csv_with_format(GAME_CSV, fmt, game_rows)
        row_indices, home_team_id, away_team_id, parent_row_idx = bs.resolve_game_context(
            game_rows, match_id, match_name
        )
        if home_name_idx is not None:
            home_team_name = game_rows[parent_row_idx][home_name_idx]
        if away_name_idx is not None:
            away_team_name = game_rows[parent_row_idx][away_name_idx]
    allowed = {team_id for team_id in [home_team_id, away_team_id] if team_id}
    mapping = bs.build_mapping(game_rows, game_rows[0], roster, row_indices, allowed)

    mapping_path = DATA_DIR / f"mapping_{match_id}.json"
    events_path = DATA_DIR / f"events_{match_id}.jsonl"
    bs.save_mapping(mapping_path, match_id, match_id, mapping)

    players = []
    sorted_players = sorted(mapping.items(), key=lambda item: item[1])
    for player_id, row_idx in sorted_players:
        info = roster.get(player_id, {})
        players.append(
            {
                "player_id": player_id,
                "name": info.get("x_name", ""),
                "jersey": info.get("x_studio_jersey_number", ""),
                "team_id": info.get("x_studio_team/id", ""),
                "row_index": row_idx,
            }
        )

    session = {
        "match_name": match_name,
        "match_id": match_id,
        "game_id": match_id,
        "mapping_path": str(mapping_path),
        "events_path": str(events_path),
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_team_name": home_team_name,
        "away_team_name": away_team_name,
        "parent_row_idx": parent_row_idx,
        "players": players,
    }
    save_session(session)
    if not events_path.exists():
        events_path.write_text("", encoding="utf-8")
    return session


def append_event(events_path: Path, event: dict) -> None:
    line = json.dumps(event, separators=(",", ":"))
    with events_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_events(events_path: Path) -> list:
    if not events_path.exists():
        return []
    events = []
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def enrich_events(events: list, session: dict) -> list:
    players = session.get("players", [])
    player_lookup = {p["player_id"]: p for p in players}
    enriched = []
    for event in events:
        info = player_lookup.get(event.get("player_id"), {})
        enriched.append(
            {
                **event,
                "player_name": info.get("name", ""),
                "player_jersey": info.get("jersey", ""),
            }
        )
    return enriched


def rewrite_events(events_path: Path, events: list) -> None:
    with events_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")


def apply_updates_for_session(session: dict, overwrite: bool = False) -> dict:
    fmt, game_rows = bs.read_csv_with_format(GAME_CSV)
    mapping = bs.load_mapping(Path(session["mapping_path"]))
    row_indices, home_team_id, away_team_id, parent_row_idx = bs.resolve_game_context(
        game_rows, session["game_id"]
    )
    bs.ensure_mapping_in_game(mapping, row_indices)

    stats = bs.compute_stats_from_events(Path(session["events_path"]), list(mapping.keys()))
    updates = bs.build_update_plan(
        game_rows,
        game_rows[0],
        mapping,
        stats,
        home_team_id,
        away_team_id,
        parent_row_idx,
        overwrite=overwrite,
    )
    bs.apply_updates(game_rows, updates)
    bs.write_csv_with_format(GAME_CSV, fmt, game_rows)
    return {"updates": len(updates)}


def build_live_stats(session: dict) -> dict:
    mapping = bs.load_mapping(Path(session["mapping_path"]))
    stats = bs.compute_stats_from_events(Path(session["events_path"]), list(mapping.keys()))
    team_points = stats.get("_team_points", {})
    home_team_id = session.get("home_team_id")
    away_team_id = session.get("away_team_id")
    home_score = int(team_points.get(home_team_id, 0)) if home_team_id else 0
    away_score = int(team_points.get(away_team_id, 0)) if away_team_id else 0

    resolved_home_id = home_team_id
    resolved_away_id = away_team_id
    if team_points and home_score == 0 and away_score == 0:
        sorted_ids = sorted(team_points.keys())
        if sorted_ids:
            resolved_home_id = sorted_ids[0]
            home_score = int(team_points.get(resolved_home_id, 0))
        if len(sorted_ids) > 1:
            resolved_away_id = sorted_ids[1]
            away_score = int(team_points.get(resolved_away_id, 0))
    player_stats = {}
    for player_id in mapping.keys():
        player_stats[player_id] = stats.get(player_id, {})
    return {
        "home_score": home_score,
        "away_score": away_score,
        "home_team_id": resolved_home_id,
        "away_team_id": resolved_away_id,
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
        if parsed.path == "/api/matches":
            matches = load_matches()
            self._send_json({"matches": matches})
            return
        if parsed.path == "/api/session":
            self._send_json({"session": load_session()})
            return
        if parsed.path == "/api/stats":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            self._send_json(build_live_stats(session))
            return
        if parsed.path == "/api/events":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            events = load_events(Path(session["events_path"]))
            self._send_json({"events": enrich_events(events, session)})
            return
        if parsed.path in ["/embed", "/embed/"]:
            embed_path = STATIC_DIR / "iframe.html"
            self._send_text(embed_path.read_text(encoding="utf-8"), content_type="text/html")
            return
        if parsed.path == "/":
            index_path = STATIC_DIR / "index.html"
            self._send_text(index_path.read_text(encoding="utf-8"), content_type="text/html")
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/start":
            data = self._read_json()
            match_name = data.get("match_name")
            if not match_name:
                self._send_json({"error": "match_name required"}, status=400)
                return
            try:
                session = build_session(match_name)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            self._send_json({"session": session})
            return
        if parsed.path == "/api/event":
            data = self._read_json()
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            player_id = data.get("player_id")
            event_type = data.get("event_type")
            period = data.get("period")
            if not player_id or not event_type or not period:
                self._send_json({"error": "player_id, event_type, period required"}, status=400)
                return
            player_info = next(
                (p for p in session.get("players", []) if p["player_id"] == player_id), None
            )
            if not player_info:
                self._send_json({"error": "Player not in session"}, status=400)
                return
            event = {
                "id": load_counter(),
                "game_id": session.get("game_id"),
                "player_id": player_id,
                "team_id": player_info.get("team_id"),
                "period": period,
                "event_type": event_type,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            if event_type == "score":
                event["points"] = int(data.get("points", 0))
            append_event(Path(session["events_path"]), event)
            save_counter(event["id"] + 1)
            self._send_json(build_live_stats(session))
            return
        if parsed.path == "/api/event/delete":
            data = self._read_json()
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            event_id = data.get("id")
            if event_id is None:
                self._send_json({"error": "id required"}, status=400)
                return
            events_path = Path(session["events_path"])
            events = load_events(events_path)
            events = [e for e in events if e.get("id") != event_id]
            rewrite_events(events_path, events)
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/event/clear":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            events_path = Path(session["events_path"])
            events_path.write_text("", encoding="utf-8")
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/finalize":
            session = load_session()
            if not session:
                self._send_json({"error": "No active session"}, status=400)
                return
            try:
                result = apply_updates_for_session(session, overwrite=True)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            self._send_json(result)
            return
        self.send_response(404)
        self.end_headers()


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Server running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
