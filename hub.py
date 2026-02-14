"""
Sports Hub — Basketball & Volleyball
Single-file backend with SQLite storage.
Run:  python hub.py
Then open http://localhost:8000
"""

import json
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "data" / "hub.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ─── Database helpers ───────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS teams (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT    NOT NULL,
        sport       TEXT    NOT NULL DEFAULT 'basketball',
        color       TEXT    NOT NULL DEFAULT '#3b82f6',
        logo_emoji  TEXT    NOT NULL DEFAULT '🏀',
        created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS players (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id     INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
        name        TEXT    NOT NULL,
        number      TEXT    NOT NULL DEFAULT '',
        position    TEXT    NOT NULL DEFAULT '',
        active      INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS matches (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        sport           TEXT    NOT NULL DEFAULT 'basketball',
        tournament_name TEXT    NOT NULL DEFAULT '',
        home_team_id    INTEGER NOT NULL REFERENCES teams(id),
        away_team_id    INTEGER NOT NULL REFERENCES teams(id),
        status          TEXT    NOT NULL DEFAULT 'pending',
        period          INTEGER NOT NULL DEFAULT 1,
        home_score      INTEGER NOT NULL DEFAULT 0,
        away_score      INTEGER NOT NULL DEFAULT 0,
        home_sets       TEXT    NOT NULL DEFAULT '[]',
        away_sets       TEXT    NOT NULL DEFAULT '[]',
        created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
        finalized_at    TEXT
    );
    CREATE TABLE IF NOT EXISTS match_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id    INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
        team        TEXT    NOT NULL,
        player_id   INTEGER REFERENCES players(id),
        player_name TEXT    NOT NULL DEFAULT '',
        event_type  TEXT    NOT NULL,
        value       INTEGER NOT NULL DEFAULT 1,
        period      INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    """)
    conn.commit()
    conn.close()


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]

# ─── Stat computation ───────────────────────────────────────────────

def compute_basketball_stats(match_id: int) -> dict:
    conn = get_db()
    match = row_to_dict(conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone())
    if not match:
        conn.close()
        return {"error": "Match not found"}
    events = rows_to_list(conn.execute(
        "SELECT * FROM match_events WHERE match_id=? ORDER BY id", (match_id,)).fetchall())
    
    home_score = 0
    away_score = 0
    home_fouls = 0
    away_fouls = 0
    player_stats = {}
    
    for ev in events:
        pid = ev["player_id"] or 0
        if pid not in player_stats:
            player_stats[pid] = {
                "player_id": pid,
                "name": ev["player_name"],
                "team": ev["team"],
                "points": 0, "fouls": 0, "assists": 0,
                "rebounds": 0, "steals": 0, "blocks": 0,
                "turnovers": 0, "free_throws_made": 0,
                "free_throws_missed": 0, "two_pt": 0, "three_pt": 0,
            }
        ps = player_stats[pid]
        et = ev["event_type"]
        val = ev["value"]
        if et == "2pt":
            ps["points"] += 2; ps["two_pt"] += 1
            if ev["team"] == "home": home_score += 2
            else: away_score += 2
        elif et == "3pt":
            ps["points"] += 3; ps["three_pt"] += 1
            if ev["team"] == "home": home_score += 3
            else: away_score += 3
        elif et == "ft_made":
            ps["points"] += 1; ps["free_throws_made"] += 1
            if ev["team"] == "home": home_score += 1
            else: away_score += 1
        elif et == "ft_missed":
            ps["free_throws_missed"] += 1
        elif et == "foul":
            ps["fouls"] += 1
            if ev["team"] == "home": home_fouls += 1
            else: away_fouls += 1
        elif et == "assist":
            ps["assists"] += 1
        elif et == "rebound":
            ps["rebounds"] += 1
        elif et == "steal":
            ps["steals"] += 1
        elif et == "block":
            ps["blocks"] += 1
        elif et == "turnover":
            ps["turnovers"] += 1
    
    # update match score
    conn.execute("UPDATE matches SET home_score=?, away_score=? WHERE id=?",
                 (home_score, away_score, match_id))
    conn.commit()
    
    home_team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (match["home_team_id"],)).fetchone())
    away_team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (match["away_team_id"],)).fetchone())
    conn.close()
    
    match["home_score"] = home_score
    match["away_score"] = away_score
    
    return {
        "match": match,
        "home_team": home_team,
        "away_team": away_team,
        "home_fouls": home_fouls,
        "away_fouls": away_fouls,
        "player_stats": list(player_stats.values()),
        "events": events,
    }


def compute_volleyball_stats(match_id: int) -> dict:
    conn = get_db()
    match = row_to_dict(conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone())
    if not match:
        conn.close()
        return {"error": "Match not found"}
    events = rows_to_list(conn.execute(
        "SELECT * FROM match_events WHERE match_id=? ORDER BY id", (match_id,)).fetchall())
    
    home_score = 0
    away_score = 0
    player_stats = {}
    
    for ev in events:
        pid = ev["player_id"] or 0
        if pid not in player_stats:
            player_stats[pid] = {
                "player_id": pid,
                "name": ev["player_name"],
                "team": ev["team"],
                "kills": 0, "errors": 0, "aces": 0,
                "blocks": 0, "digs": 0, "assists": 0,
                "service_errors": 0, "points": 0,
            }
        ps = player_stats[pid]
        et = ev["event_type"]
        if et == "kill":
            ps["kills"] += 1; ps["points"] += 1
            if ev["team"] == "home": home_score += 1
            else: away_score += 1
        elif et == "ace":
            ps["aces"] += 1; ps["points"] += 1
            if ev["team"] == "home": home_score += 1
            else: away_score += 1
        elif et == "block_point":
            ps["blocks"] += 1; ps["points"] += 1
            if ev["team"] == "home": home_score += 1
            else: away_score += 1
        elif et == "opponent_error":
            if ev["team"] == "home": home_score += 1
            else: away_score += 1
        elif et == "error":
            ps["errors"] += 1
        elif et == "service_error":
            ps["service_errors"] += 1
        elif et == "dig":
            ps["digs"] += 1
        elif et == "assist":
            ps["assists"] += 1
    
    conn.execute("UPDATE matches SET home_score=?, away_score=? WHERE id=?",
                 (home_score, away_score, match_id))
    conn.commit()
    
    home_team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (match["home_team_id"],)).fetchone())
    away_team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (match["away_team_id"],)).fetchone())
    conn.close()
    
    match["home_score"] = home_score
    match["away_score"] = away_score
    
    return {
        "match": match,
        "home_team": home_team,
        "away_team": away_team,
        "player_stats": list(player_stats.values()),
        "events": events,
    }


def compute_stats(match_id: int) -> dict:
    conn = get_db()
    match = row_to_dict(conn.execute("SELECT sport FROM matches WHERE id=?", (match_id,)).fetchone())
    conn.close()
    if not match:
        return {"error": "Match not found"}
    if match["sport"] == "volleyball":
        return compute_volleyball_stats(match_id)
    return compute_basketball_stats(match_id)

# ─── HTTP Server ────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence request logs

    def _json(self, data, status=200):
        payload = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _html(self, path):
        if path.exists():
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def _read_body(self):
        ln = int(self.headers.get("Content-Length", "0"))
        if ln == 0:
            return {}
        return json.loads(self.rfile.read(ln).decode())

    # ── GET ──
    def do_GET(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/") or "/"
        qs = parse_qs(p.query)

        # Pages
        if path == "/":
            self._html(STATIC_DIR / "hub_index.html"); return
        if path == "/teams":
            self._html(STATIC_DIR / "hub_teams.html"); return
        if path == "/match":
            self._html(STATIC_DIR / "hub_match.html"); return
        if path == "/overlay":
            self._html(STATIC_DIR / "hub_overlay.html"); return
        if path == "/desk":
            self._html(STATIC_DIR / "hub_desk.html"); return
        if path == "/stream-guide":
            self._html(STATIC_DIR / "hub_stream_guide.html"); return

        # API
        if path == "/api/teams":
            conn = get_db()
            sport = qs.get("sport", [None])[0]
            if sport:
                teams = rows_to_list(conn.execute(
                    "SELECT * FROM teams WHERE sport=? ORDER BY name", (sport,)).fetchall())
            else:
                teams = rows_to_list(conn.execute("SELECT * FROM teams ORDER BY name").fetchall())
            conn.close()
            self._json({"teams": teams}); return

        if path == "/api/players":
            conn = get_db()
            team_id = qs.get("team_id", [None])[0]
            if team_id:
                players = rows_to_list(conn.execute(
                    "SELECT * FROM players WHERE team_id=? AND active=1 ORDER BY number, name",
                    (team_id,)).fetchall())
            else:
                players = rows_to_list(conn.execute(
                    "SELECT * FROM players WHERE active=1 ORDER BY name").fetchall())
            conn.close()
            self._json({"players": players}); return

        if path == "/api/matches":
            conn = get_db()
            sport = qs.get("sport", [None])[0]
            if sport:
                matches = rows_to_list(conn.execute("""
                    SELECT m.*, ht.name as home_name, ht.color as home_color, ht.logo_emoji as home_emoji,
                           at.name as away_name, at.color as away_color, at.logo_emoji as away_emoji
                    FROM matches m
                    JOIN teams ht ON m.home_team_id=ht.id
                    JOIN teams at ON m.away_team_id=at.id
                    WHERE m.sport=?
                    ORDER BY m.id DESC
                """, (sport,)).fetchall())
            else:
                matches = rows_to_list(conn.execute("""
                    SELECT m.*, ht.name as home_name, ht.color as home_color, ht.logo_emoji as home_emoji,
                           at.name as away_name, at.color as away_color, at.logo_emoji as away_emoji
                    FROM matches m
                    JOIN teams ht ON m.home_team_id=ht.id
                    JOIN teams at ON m.away_team_id=at.id
                    ORDER BY m.id DESC
                """).fetchall())
            conn.close()
            self._json({"matches": matches}); return

        if path == "/api/match":
            mid = qs.get("id", [None])[0]
            if not mid:
                self._json({"error": "id required"}, 400); return
            result = compute_stats(int(mid))
            # Also fetch roster
            conn = get_db()
            match = row_to_dict(conn.execute("SELECT * FROM matches WHERE id=?", (mid,)).fetchone())
            if match:
                home_players = rows_to_list(conn.execute(
                    "SELECT * FROM players WHERE team_id=? AND active=1 ORDER BY number, name",
                    (match["home_team_id"],)).fetchall())
                away_players = rows_to_list(conn.execute(
                    "SELECT * FROM players WHERE team_id=? AND active=1 ORDER BY number, name",
                    (match["away_team_id"],)).fetchall())
                result["home_players"] = home_players
                result["away_players"] = away_players
            conn.close()
            self._json(result); return

        if path == "/api/events":
            mid = qs.get("match_id", [None])[0]
            if not mid:
                self._json({"error": "match_id required"}, 400); return
            conn = get_db()
            events = rows_to_list(conn.execute(
                "SELECT * FROM match_events WHERE match_id=? ORDER BY id DESC", (mid,)).fetchall())
            conn.close()
            self._json({"events": events}); return

        self.send_response(404)
        self.end_headers()

    # ── POST ──
    def do_POST(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/api/teams":
            d = self._read_body()
            conn = get_db()
            cur = conn.execute(
                "INSERT INTO teams (name, sport, color, logo_emoji) VALUES (?,?,?,?)",
                (d.get("name",""), d.get("sport","basketball"),
                 d.get("color","#3b82f6"), d.get("logo_emoji","🏀")))
            conn.commit()
            team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (cur.lastrowid,)).fetchone())
            conn.close()
            self._json({"team": team}); return

        if path == "/api/teams/update":
            d = self._read_body()
            conn = get_db()
            conn.execute("UPDATE teams SET name=?, color=?, logo_emoji=? WHERE id=?",
                         (d.get("name"), d.get("color"), d.get("logo_emoji"), d.get("id")))
            conn.commit()
            team = row_to_dict(conn.execute("SELECT * FROM teams WHERE id=?", (d.get("id"),)).fetchone())
            conn.close()
            self._json({"team": team}); return

        if path == "/api/teams/delete":
            d = self._read_body()
            conn = get_db()
            conn.execute("DELETE FROM teams WHERE id=?", (d.get("id"),))
            conn.commit()
            conn.close()
            self._json({"ok": True}); return

        if path == "/api/players":
            d = self._read_body()
            conn = get_db()
            cur = conn.execute(
                "INSERT INTO players (team_id, name, number, position) VALUES (?,?,?,?)",
                (d.get("team_id"), d.get("name",""), d.get("number",""), d.get("position","")))
            conn.commit()
            player = row_to_dict(conn.execute("SELECT * FROM players WHERE id=?", (cur.lastrowid,)).fetchone())
            conn.close()
            self._json({"player": player}); return

        if path == "/api/players/update":
            d = self._read_body()
            conn = get_db()
            conn.execute("UPDATE players SET name=?, number=?, position=? WHERE id=?",
                         (d.get("name"), d.get("number"), d.get("position"), d.get("id")))
            conn.commit()
            conn.close()
            self._json({"ok": True}); return

        if path == "/api/players/delete":
            d = self._read_body()
            conn = get_db()
            conn.execute("UPDATE players SET active=0 WHERE id=?", (d.get("id"),))
            conn.commit()
            conn.close()
            self._json({"ok": True}); return

        if path == "/api/matches":
            d = self._read_body()
            conn = get_db()
            cur = conn.execute(
                "INSERT INTO matches (sport, tournament_name, home_team_id, away_team_id) VALUES (?,?,?,?)",
                (d.get("sport","basketball"), d.get("tournament_name",""),
                 d.get("home_team_id"), d.get("away_team_id")))
            conn.commit()
            mid = cur.lastrowid
            conn.close()
            self._json({"match_id": mid}); return

        if path == "/api/matches/update":
            d = self._read_body()
            conn = get_db()
            fields = []
            vals = []
            for k in ("status","period","tournament_name","home_sets","away_sets","finalized_at"):
                if k in d:
                    fields.append(f"{k}=?")
                    vals.append(d[k])
            if fields:
                vals.append(d["id"])
                conn.execute(f"UPDATE matches SET {','.join(fields)} WHERE id=?", vals)
                conn.commit()
            conn.close()
            self._json({"ok": True}); return

        if path == "/api/event":
            d = self._read_body()
            conn = get_db()
            conn.execute(
                "INSERT INTO match_events (match_id, team, player_id, player_name, event_type, value, period) VALUES (?,?,?,?,?,?,?)",
                (d.get("match_id"), d.get("team",""), d.get("player_id"),
                 d.get("player_name",""), d.get("event_type",""),
                 d.get("value", 1), d.get("period", 1)))
            conn.commit()
            conn.close()
            result = compute_stats(int(d["match_id"]))
            self._json(result); return

        if path == "/api/event/delete":
            d = self._read_body()
            conn = get_db()
            ev = row_to_dict(conn.execute("SELECT match_id FROM match_events WHERE id=?", (d.get("id"),)).fetchone())
            conn.execute("DELETE FROM match_events WHERE id=?", (d.get("id"),))
            conn.commit()
            conn.close()
            if ev:
                result = compute_stats(ev["match_id"])
                self._json(result); return
            self._json({"ok": True}); return

        if path == "/api/event/clear":
            d = self._read_body()
            conn = get_db()
            conn.execute("DELETE FROM match_events WHERE match_id=?", (d.get("match_id"),))
            conn.execute("UPDATE matches SET home_score=0, away_score=0 WHERE id=?", (d.get("match_id"),))
            conn.commit()
            conn.close()
            self._json({"ok": True}); return

        self.send_response(404)
        self.end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def main():
    init_db()
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Sports Hub running at http://{host}:{port}")
    print(f"  Local: http://localhost:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
