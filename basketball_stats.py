import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


GAME_ID_REGEX = re.compile(r"x_game_(\d+)_")

LOGICAL_COLUMNS = {
    "game_id": ["id"],
    "home_team": ["x_studio_home_team/id", "Home Team/ID"],
    "home_team_name": ["x_studio_home_team/x_name", "Home Team/Name", "Home Team/Nombre"],
    "away_team": ["x_studio_away_team/id", "Away Team/ID"],
    "away_team_name": ["x_studio_away_team/x_name", "Away Team/Name", "Away Team/Nombre"],
    "home_score": ["x_studio_home_score", "Home Score"],
    "away_score": ["x_studio_away_score", "Away Score"],
    "player_name": ["x_studio_stats/x_studio_player", "Stats/Player"],
    "player_id": ["x_studio_stats/x_studio_player/id", "Stats/Player/ID"],
    "player_team": ["x_studio_stats/x_studio_team/id", "Stats/Team/ID"],
    "player_jersey": ["x_studio_stats/x_studio_jersey_number", "Stats/Jersey"],
    "q1": ["x_studio_stats/x_studio_q1", "Stats/Q1"],
    "q2": ["x_studio_stats/x_studio_q2", "Stats/Q2"],
    "q3": ["x_studio_stats/x_studio_q3", "Stats/Q3"],
    "q4": ["x_studio_stats/x_studio_q4", "Stats/Q4"],
    "ot": ["x_studio_stats/x_studio_ot", "Stats/OT"],
    "fouls": ["x_studio_stats/x_studio_fouls", "Stats/Fouls"],
}

MATCH_COLUMNS = {
    "match_id": ["ID", "id"],
    "match_name": ["Match Name", "x_studio_match_name"],
}


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def detect_line_ending(text: str) -> str:
    return "\r\n" if "\r\n" in text else "\n"


def detect_delimiter(line: str) -> str:
    candidates = [",", ";", "\t", "|"]
    best = ","
    best_count = -1
    for cand in candidates:
        count = 0
        in_quote = False
        i = 0
        while i < len(line):
            ch = line[i]
            if ch == '"':
                if in_quote and i + 1 < len(line) and line[i + 1] == '"':
                    i += 2
                    continue
                in_quote = not in_quote
            elif ch == cand and not in_quote:
                count += 1
            i += 1
        if count > best_count:
            best_count = count
            best = cand
    return best


def detect_quoting(line: str) -> int:
    line = line.strip("\r\n")
    if line.startswith('"') and line.endswith('"'):
        if '\",\"' in line or '\";\"' in line or '\"\t\"' in line or '\"|\"' in line:
            return csv.QUOTE_ALL
    return csv.QUOTE_MINIMAL


@dataclass
class CsvFormat:
    delimiter: str
    quotechar: str
    quoting: int
    lineterminator: str


def read_csv_with_format(path: Path) -> Tuple[CsvFormat, List[List[str]]]:
    text = read_text(path)
    if not text:
        raise ValueError(f"Empty CSV: {path}")
    first_line = text.splitlines()[0]
    delimiter = detect_delimiter(first_line)
    quoting = detect_quoting(first_line)
    lineterminator = detect_line_ending(text)

    rows: List[List[str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar='"')
        for row in reader:
            rows.append(row)

    if rows:
        header_len = len(rows[0])
        for i in range(1, len(rows)):
            if len(rows[i]) < header_len:
                rows[i] = rows[i] + [""] * (header_len - len(rows[i]))
            elif len(rows[i]) > header_len:
                rows[i] = rows[i][:header_len]

    return CsvFormat(delimiter, '"', quoting, lineterminator), rows


def write_csv_with_format(path: Path, fmt: CsvFormat, rows: List[List[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(
            f,
            delimiter=fmt.delimiter,
            quotechar=fmt.quotechar,
            quoting=fmt.quoting,
            lineterminator=fmt.lineterminator,
        )
        for row in rows:
            writer.writerow(row)


def index_of(headers: List[str], name: str) -> int:
    for i, h in enumerate(headers):
        if h == name:
            return i
    raise KeyError(f"Missing column: {name}")


def optional_index(headers: List[str], name: str) -> Optional[int]:
    for i, h in enumerate(headers):
        if h == name:
            return i
    return None


def find_header_index(headers: List[str], aliases: List[str]) -> Optional[int]:
    for name in aliases:
        idx = optional_index(headers, name)
        if idx is not None:
            return idx
    return None


def require_header_index(headers: List[str], aliases: List[str], label: str) -> int:
    idx = find_header_index(headers, aliases)
    if idx is None:
        raise ValueError(f"Missing column for {label}; expected one of: {', '.join(aliases)}")
    return idx


def is_missing(value: str) -> bool:
    if value is None:
        return True
    stripped = value.strip()
    if stripped == "":
        return True
    lowered = stripped.lower()
    return lowered in {"nan", "null"}


def extract_game_id(raw_id: str) -> Optional[str]:
    if not raw_id:
        return None
    match = GAME_ID_REGEX.search(raw_id)
    return match.group(1) if match else None


def normalize_team_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


def parse_match_teams(match_name: str) -> Optional[Tuple[str, str]]:
    if not match_name:
        return None
    lowered = match_name.strip()
    parts = re.split(r"\s+vs\s+", lowered, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None
    left = parts[0].strip()
    right = parts[1].strip()
    right = re.sub(r"\s+\d{4}-\d{2}-\d{2}.*$", "", right).strip()
    left = re.sub(r"\s+\d{4}-\d{2}-\d{2}.*$", "", left).strip()
    if not left or not right:
        return None
    return left, right


def resolve_match_id(matches_rows: List[List[str]], match_name: str) -> str:
    headers = matches_rows[0]
    id_idx = find_header_index(headers, MATCH_COLUMNS["match_id"])
    name_idx = find_header_index(headers, MATCH_COLUMNS["match_name"])
    if id_idx is None or name_idx is None:
        raise ValueError("Matches Name.csv is missing match id or match name column.")
    for row in matches_rows[1:]:
        if row[name_idx] == match_name:
            raw_id = row[id_idx]
            if raw_id.isdigit():
                return raw_id
            parsed = extract_game_id(raw_id)
            if parsed:
                return parsed
            return raw_id
    raise ValueError(f"Match Name not found: {match_name}")


def assign_game_ids(rows: List[List[str]], id_idx: int) -> List[Optional[str]]:
    game_ids: List[Optional[str]] = []
    current_game_id: Optional[str] = None
    for row in rows:
        raw_id = row[id_idx]
        parsed = extract_game_id(raw_id) if raw_id else None
        if parsed:
            current_game_id = parsed
        game_ids.append(current_game_id)
    return game_ids


def build_roster(players_rows: List[List[str]]) -> Dict[str, Dict[str, str]]:
    headers = players_rows[0]
    id_idx = index_of(headers, "id")
    name_idx = index_of(headers, "x_name")
    jersey_idx = index_of(headers, "x_studio_jersey_number")
    team_idx = index_of(headers, "x_studio_team/id")

    roster: Dict[str, Dict[str, str]] = {}
    for row in players_rows[1:]:
        player_id = row[id_idx]
        if not player_id:
            continue
        roster[player_id] = {
            "x_name": row[name_idx],
            "x_studio_jersey_number": row[jersey_idx],
            "x_studio_team/id": row[team_idx],
        }
    return roster


def detect_duplicate_names(roster: Dict[str, Dict[str, str]]) -> Dict[str, List[str]]:
    name_to_ids: Dict[str, List[str]] = {}
    for player_id, info in roster.items():
        name = info["x_name"]
        name_to_ids.setdefault(name, []).append(player_id)
    return {name: ids for name, ids in name_to_ids.items() if len(ids) > 1}


def expand_game_rows(
    rows: List[List[str]],
    headers: List[str],
    roster: Dict[str, Dict[str, str]],
    row_indices: List[int],
    home_team_id: Optional[str],
    away_team_id: Optional[str],
) -> int:
    if not row_indices:
        return 0

    player_id_col = find_header_index(headers, LOGICAL_COLUMNS["player_id"])
    player_name_col = find_header_index(headers, LOGICAL_COLUMNS["player_name"])
    team_col = find_header_index(headers, LOGICAL_COLUMNS["player_team"])
    jersey_col = find_header_index(headers, LOGICAL_COLUMNS["player_jersey"])

    if player_id_col is None and player_name_col is None:
        raise ValueError("Missing player identifier column in game CSV.")

    existing_player_ids = set()
    existing_player_names = set()
    for idx in row_indices:
        row = rows[idx]
        if player_id_col is not None:
            value = row[player_id_col].strip()
            if value:
                existing_player_ids.add(value)
        if player_name_col is not None:
            value = row[player_name_col].strip()
            if value:
                existing_player_names.add(value)

    target_teams = {team for team in [home_team_id, away_team_id] if team}
    if not target_teams:
        return 0

    def jersey_sort_key(value: str) -> Tuple[int, str]:
        try:
            return (0, f"{int(value):04d}")
        except ValueError:
            return (1, value)

    candidates = [
        (player_id, info)
        for player_id, info in roster.items()
        if info.get("x_studio_team/id") in target_teams
    ]
    candidates.sort(
        key=lambda item: (
            item[1].get("x_studio_team/id", ""),
            jersey_sort_key(item[1].get("x_studio_jersey_number", "")),
            item[1].get("x_name", ""),
        )
    )

    new_rows: List[List[str]] = []
    parent_row_idx = row_idx_from_parent(rows, headers, row_indices)
    parent_row = rows[parent_row_idx]
    parent_has_player = False
    if player_id_col is not None and parent_row[player_id_col].strip():
        parent_has_player = True
    if player_name_col is not None and parent_row[player_name_col].strip():
        parent_has_player = True

    missing_candidates = []
    for player_id, info in candidates:
        if player_id_col is not None:
            if player_id in existing_player_ids:
                continue
        else:
            name_value = info.get("x_name", "")
            if name_value in existing_player_names:
                continue
        missing_candidates.append((player_id, info))

    if not parent_has_player and missing_candidates:
        player_id, info = missing_candidates.pop(0)
        if player_id_col is not None:
            parent_row[player_id_col] = player_id
        if player_name_col is not None:
            parent_row[player_name_col] = info.get("x_name", "")
        if team_col is not None:
            parent_row[team_col] = info.get("x_studio_team/id", "")
        if jersey_col is not None:
            parent_row[jersey_col] = info.get("x_studio_jersey_number", "")
        existing_player_ids.add(player_id)
        existing_player_names.add(info.get("x_name", ""))

    for player_id, info in missing_candidates:
        if player_id_col is not None:
            if player_id in existing_player_ids:
                continue
        else:
            name_value = info.get("x_name", "")
            if name_value in existing_player_names:
                continue

        row = [""] * len(headers)
        if player_id_col is not None:
            row[player_id_col] = player_id
        if player_name_col is not None:
            row[player_name_col] = info.get("x_name", "")
        if team_col is not None:
            row[team_col] = info.get("x_studio_team/id", "")
        if jersey_col is not None:
            row[jersey_col] = info.get("x_studio_jersey_number", "")
        new_rows.append(row)

    if not new_rows:
        return 0

    insert_at = max(row_indices) + 1
    rows[insert_at:insert_at] = new_rows
    return len(new_rows)


def build_mapping(
    game_rows: List[List[str]],
    game_headers: List[str],
    roster: Dict[str, Dict[str, str]],
    row_indices: List[int],
    allowed_team_ids: Optional[set] = None,
) -> Dict[str, int]:
    player_id_col = find_header_index(game_headers, LOGICAL_COLUMNS["player_id"])
    player_name_col = find_header_index(game_headers, LOGICAL_COLUMNS["player_name"])
    team_col = find_header_index(game_headers, LOGICAL_COLUMNS["player_team"])
    jersey_col = find_header_index(game_headers, LOGICAL_COLUMNS["player_jersey"])

    mapping: Dict[str, int] = {}
    for idx in row_indices:
        row = game_rows[idx]
        if player_id_col is not None:
            player_id = row[player_id_col].strip()
            if not player_id:
                continue
            if player_id not in roster:
                raise ValueError(f"Player ID not found in roster: {player_id}")
            if allowed_team_ids:
                player_team_id = roster[player_id].get("x_studio_team/id")
                if player_team_id not in allowed_team_ids:
                    continue
        else:
            if player_name_col is None:
                raise ValueError("Missing player identifier column in game CSV.")
            player_name = row[player_name_col].strip()
            if not player_name:
                continue

            candidates = [
                (player_id, info)
                for player_id, info in roster.items()
                if info["x_name"] == player_name
            ]
            if not candidates:
                raise ValueError(f"No roster match for stat row name: {player_name}")

            if len(candidates) > 1:
                if team_col is not None and jersey_col is not None:
                    row_team = row[team_col].strip()
                    row_jersey = row[jersey_col].strip()
                    candidates = [
                        (player_id, info)
                        for player_id, info in candidates
                        if info["x_studio_team/id"] == row_team
                        and info["x_studio_jersey_number"] == row_jersey
                    ]
                if len(candidates) != 1:
                    raise ValueError(
                        "Ambiguous player name in roster; add team/jersey disambiguation before start: "
                        + player_name
                    )

            player_id = candidates[0][0]
            if allowed_team_ids:
                player_team_id = roster[player_id].get("x_studio_team/id")
                if player_team_id not in allowed_team_ids:
                    continue
        if player_id in mapping and mapping[player_id] != idx:
            continue
        mapping[player_id] = idx

    return mapping


def validate_required_columns(headers: List[str], required: List[str], label: str) -> None:
    missing = [name for name in required if name not in headers]
    if missing:
        raise ValueError(f"Missing columns in {label}: {', '.join(missing)}")


def validate_any_column(headers: List[str], aliases: List[str], label: str) -> None:
    if find_header_index(headers, aliases) is None:
        raise ValueError(f"Missing column for {label}; expected one of: {', '.join(aliases)}")


def compute_stats_from_events(
    events_path: Path,
    mapped_player_ids: List[str],
) -> Dict[str, Dict[str, int]]:
    stats: Dict[str, Dict[str, int]] = {}
    for player_id in mapped_player_ids:
        stats[player_id] = {
            "q1": 0,
            "q2": 0,
            "q3": 0,
            "q4": 0,
            "ot": 0,
            "fouls": 0,
        }

    team_points: Dict[str, int] = {}

    if not events_path.exists():
        return stats

    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            player_id = event.get("player_id")
            period = event.get("period")
            event_type = event.get("event_type")
            team_id = event.get("team_id")

            if player_id not in stats:
                continue

            if event_type == "foul":
                stats[player_id]["fouls"] += 1
                continue

            if event_type == "score":
                points = int(event.get("points", 0))
                period_key = normalize_period(period)
                if period_key:
                    stats[player_id][period_key] += points
                if team_id:
                    team_points[team_id] = team_points.get(team_id, 0) + points

    stats["_team_points"] = team_points
    return stats


def normalize_period(period) -> Optional[str]:
    if period is None:
        return None
    if isinstance(period, int):
        if period == 1:
            return "q1"
        if period == 2:
            return "q2"
        if period == 3:
            return "q3"
        if period == 4:
            return "q4"
        return "ot"
    if isinstance(period, str):
        lowered = period.strip().lower()
        if lowered in {"q1", "1"}:
            return "q1"
        if lowered in {"q2", "2"}:
            return "q2"
        if lowered in {"q3", "3"}:
            return "q3"
        if lowered in {"q4", "4"}:
            return "q4"
        if lowered in {"ot", "overtime"}:
            return "ot"
    return None


def build_update_plan(
    rows: List[List[str]],
    headers: List[str],
    mapping: Dict[str, int],
    stats: Dict[str, Dict[str, int]],
    home_team_id: Optional[str],
    away_team_id: Optional[str],
    parent_row_idx: int,
    overwrite: bool = False,
) -> List[Tuple[int, int, str, str]]:
    updates: List[Tuple[int, int, str, str]] = []
    header_map = {name: i for i, name in enumerate(headers)}

    stat_cols = [
        ("q1", LOGICAL_COLUMNS["q1"]),
        ("q2", LOGICAL_COLUMNS["q2"]),
        ("q3", LOGICAL_COLUMNS["q3"]),
        ("q4", LOGICAL_COLUMNS["q4"]),
        ("ot", LOGICAL_COLUMNS["ot"]),
        ("fouls", LOGICAL_COLUMNS["fouls"]),
    ]

    for player_id, row_idx in mapping.items():
        player_stats = stats.get(player_id, {})
        for stat_key, aliases in stat_cols:
            col_name = None
            for alias in aliases:
                if alias in header_map:
                    col_name = alias
                    break
            if col_name is None:
                continue
            col_idx = header_map[col_name]
            current = rows[row_idx][col_idx]
            if overwrite or is_missing(current):
                new_value = str(player_stats.get(stat_key, 0))
                updates.append((row_idx, col_idx, current, new_value))

    if home_team_id and away_team_id:
        team_points = stats.get("_team_points", {})
        home_score = str(team_points.get(home_team_id, 0))
        away_score = str(team_points.get(away_team_id, 0))

        for aliases, new_value in [
            (LOGICAL_COLUMNS["home_score"], home_score),
            (LOGICAL_COLUMNS["away_score"], away_score),
        ]:
            col_name = None
            for alias in aliases:
                if alias in header_map:
                    col_name = alias
                    break
            if col_name is None:
                continue
            col_idx = header_map[col_name]
            current = rows[parent_row_idx][col_idx]
            if overwrite or is_missing(current):
                updates.append((parent_row_idx, col_idx, current, new_value))

    return updates


def row_idx_from_parent(rows: List[List[str]], headers: List[str], row_indices: List[int]) -> int:
    id_idx = find_header_index(headers, LOGICAL_COLUMNS["game_id"])
    if id_idx is None:
        return row_indices[0]
    for i in row_indices:
        if rows[i][id_idx].strip():
            return i
    return row_indices[0]


def apply_updates(rows: List[List[str]], updates: List[Tuple[int, int, str, str]]) -> None:
    for row_idx, col_idx, _old, new in updates:
        rows[row_idx][col_idx] = new


def select_game_rows_by_match_name(game_rows: List[List[str]], match_name: str) -> List[int]:
    headers = game_rows[0]
    home_id_idx = find_header_index(headers, LOGICAL_COLUMNS["home_team"])
    away_id_idx = find_header_index(headers, LOGICAL_COLUMNS["away_team"])
    home_name_idx = find_header_index(headers, LOGICAL_COLUMNS["home_team_name"])
    away_name_idx = find_header_index(headers, LOGICAL_COLUMNS["away_team_name"])

    teams = parse_match_teams(match_name)
    if not teams:
        raise ValueError("Unable to parse teams from match name.")
    match_home, match_away = [normalize_team_name(t) for t in teams]

    parent_indices = []
    for i in range(1, len(game_rows)):
        row = game_rows[i]
        is_parent = False
        if home_id_idx is not None and row[home_id_idx].strip():
            is_parent = True
        if away_id_idx is not None and row[away_id_idx].strip():
            is_parent = True
        if home_name_idx is not None and row[home_name_idx].strip():
            is_parent = True
        if away_name_idx is not None and row[away_name_idx].strip():
            is_parent = True
        if is_parent:
            parent_indices.append(i)

    if not parent_indices:
        raise ValueError("No game header rows found to match by team name.")

    parent_indices.append(len(game_rows))
    for idx in range(len(parent_indices) - 1):
        start = parent_indices[idx]
        end = parent_indices[idx + 1]
        parent_row = game_rows[start]
        home_name = ""
        away_name = ""
        if home_name_idx is not None:
            home_name = parent_row[home_name_idx]
        if away_name_idx is not None:
            away_name = parent_row[away_name_idx]
        if home_name and away_name:
            home_norm = normalize_team_name(home_name)
            away_norm = normalize_team_name(away_name)
            if home_norm == match_home and away_norm == match_away:
                return list(range(start, end))
            if home_norm == match_away and away_norm == match_home:
                return list(range(start, end))

    raise ValueError("No matching game rows found for selected match name.")


def select_game_rows(game_rows: List[List[str]], game_id: str, match_name: Optional[str]) -> List[int]:
    headers = game_rows[0]
    id_idx = find_header_index(headers, LOGICAL_COLUMNS["game_id"])
    if id_idx is None:
        if match_name:
            return select_game_rows_by_match_name(game_rows, match_name)
        return list(range(1, len(game_rows)))
    assigned_game_ids = assign_game_ids(game_rows[1:], id_idx)
    selected = [i + 1 for i, gid in enumerate(assigned_game_ids) if gid == game_id]
    if not selected and match_name:
        return select_game_rows_by_match_name(game_rows, match_name)
    return selected


def resolve_game_context(
    game_rows: List[List[str]], game_id: str, match_name: Optional[str] = None
) -> Tuple[List[int], Optional[str], Optional[str], int]:
    headers = game_rows[0]
    row_indices = select_game_rows(game_rows, game_id, match_name)
    if not row_indices:
        raise ValueError(f"No game rows found for game_id: {game_id}")

    home_team_id = None
    away_team_id = None
    home_idx = find_header_index(headers, LOGICAL_COLUMNS["home_team"])
    away_idx = find_header_index(headers, LOGICAL_COLUMNS["away_team"])
    if home_idx is not None and away_idx is not None:
        parent_row_idx = row_idx_from_parent(game_rows, headers, row_indices)
        parent_row = game_rows[parent_row_idx]
        home_team_id = parent_row[home_idx]
        away_team_id = parent_row[away_idx]
    else:
        parent_row_idx = row_idx_from_parent(game_rows, headers, row_indices)

    return row_indices, home_team_id, away_team_id, parent_row_idx


def save_mapping(path: Path, match_id: str, game_id: str, mapping: Dict[str, int]) -> None:
    data = {"match_id": match_id, "game_id": game_id, "mapping": mapping}
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_mapping(path: Path) -> Dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {k: int(v) for k, v in data["mapping"].items()}


def ensure_mapping_in_game(mapping: Dict[str, int], row_indices: List[int]) -> None:
    allowed = set(row_indices)
    out_of_scope = [pid for pid, idx in mapping.items() if idx not in allowed]
    if out_of_scope:
        raise ValueError("Mapping contains rows outside selected game.")


def cmd_validate(args: argparse.Namespace) -> None:
    _, matches_rows = read_csv_with_format(Path(args.matches))
    _, players_rows = read_csv_with_format(Path(args.players))
    _, game_rows = read_csv_with_format(Path(args.game))

    validate_any_column(matches_rows[0], MATCH_COLUMNS["match_id"], "match id")
    validate_any_column(matches_rows[0], MATCH_COLUMNS["match_name"], "match name")
    validate_required_columns(
        players_rows[0],
        ["id", "x_name", "x_studio_jersey_number", "x_studio_team/id"],
        "Player (x_player).csv",
    )
    if find_header_index(game_rows[0], LOGICAL_COLUMNS["player_id"]) is None:
        validate_any_column(game_rows[0], LOGICAL_COLUMNS["player_name"], "player name")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["q1"], "q1")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["q2"], "q2")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["q3"], "q3")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["q4"], "q4")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["ot"], "ot")
    validate_any_column(game_rows[0], LOGICAL_COLUMNS["fouls"], "fouls")

    match_id = resolve_match_id(matches_rows, args.match_name)
    row_indices, _home_id, _away_id, _parent_row_idx = resolve_game_context(
        game_rows, match_id, args.match_name
    )

    roster = build_roster(players_rows)
    allowed = {team_id for team_id in [_home_id, _away_id] if team_id}
    _mapping = build_mapping(game_rows, game_rows[0], roster, row_indices, allowed)

    print(f"OK: match_id={match_id} game_rows={len(row_indices)} mapped={len(_mapping)}")


def cmd_build_mapping(args: argparse.Namespace) -> None:
    _, matches_rows = read_csv_with_format(Path(args.matches))
    _, players_rows = read_csv_with_format(Path(args.players))
    _, game_rows = read_csv_with_format(Path(args.game))

    match_id = resolve_match_id(matches_rows, args.match_name)
    row_indices, _home_id, _away_id, _parent_row_idx = resolve_game_context(
        game_rows, match_id, args.match_name
    )

    roster = build_roster(players_rows)
    allowed = {team_id for team_id in [_home_id, _away_id] if team_id}
    mapping = build_mapping(game_rows, game_rows[0], roster, row_indices, allowed)

    save_mapping(Path(args.mapping), match_id, match_id, mapping)
    print(f"Saved mapping: {args.mapping}")


def cmd_dry_run(args: argparse.Namespace) -> None:
    fmt, game_rows = read_csv_with_format(Path(args.game))
    mapping = load_mapping(Path(args.mapping))
    row_indices, home_team_id, away_team_id, parent_row_idx = resolve_game_context(
        game_rows, args.game_id, args.match_name
    )
    ensure_mapping_in_game(mapping, row_indices)

    stats = compute_stats_from_events(Path(args.events), list(mapping.keys()))
    updates = build_update_plan(
        game_rows,
        game_rows[0],
        mapping,
        stats,
        home_team_id,
        away_team_id,
        parent_row_idx,
        overwrite=args.overwrite,
    )

    print(f"Updates: {len(updates)}")
    for row_idx, col_idx, old, new in updates:
        col_name = game_rows[0][col_idx]
        print(f"row={row_idx} col={col_name} old={old!r} new={new!r}")


def cmd_apply(args: argparse.Namespace) -> None:
    fmt, game_rows = read_csv_with_format(Path(args.game))
    mapping = load_mapping(Path(args.mapping))
    row_indices, home_team_id, away_team_id, parent_row_idx = resolve_game_context(
        game_rows, args.game_id, args.match_name
    )
    ensure_mapping_in_game(mapping, row_indices)

    stats = compute_stats_from_events(Path(args.events), list(mapping.keys()))
    updates = build_update_plan(
        game_rows,
        game_rows[0],
        mapping,
        stats,
        home_team_id,
        away_team_id,
        parent_row_idx,
        overwrite=args.overwrite,
    )
    apply_updates(game_rows, updates)

    output_path = Path(args.output) if args.output else Path(args.game)
    write_csv_with_format(output_path, fmt, game_rows)
    print(f"Wrote: {output_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline-first basketball stats CSV updater")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="Validate schemas and mapping")
    validate.add_argument("--matches", required=True)
    validate.add_argument("--players", required=True)
    validate.add_argument("--game", required=True)
    validate.add_argument("--match-name", required=True)
    validate.set_defaults(func=cmd_validate)

    mapping = sub.add_parser("build-mapping", help="Build player_id to row mapping")
    mapping.add_argument("--matches", required=True)
    mapping.add_argument("--players", required=True)
    mapping.add_argument("--game", required=True)
    mapping.add_argument("--match-name", required=True)
    mapping.add_argument("--mapping", required=True)
    mapping.set_defaults(func=cmd_build_mapping)

    dry_run = sub.add_parser("dry-run", help="Show planned updates")
    dry_run.add_argument("--game", required=True)
    dry_run.add_argument("--mapping", required=True)
    dry_run.add_argument("--events", required=True)
    dry_run.add_argument("--game-id", required=True)
    dry_run.add_argument("--match-name")
    dry_run.add_argument("--overwrite", action="store_true")
    dry_run.set_defaults(func=cmd_dry_run)

    apply_cmd = sub.add_parser("apply", help="Apply updates to CSV")
    apply_cmd.add_argument("--game", required=True)
    apply_cmd.add_argument("--mapping", required=True)
    apply_cmd.add_argument("--events", required=True)
    apply_cmd.add_argument("--game-id", required=True)
    apply_cmd.add_argument("--output")
    apply_cmd.add_argument("--match-name")
    apply_cmd.add_argument("--overwrite", action="store_true")
    apply_cmd.set_defaults(func=cmd_apply)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
