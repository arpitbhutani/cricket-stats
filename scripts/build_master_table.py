"""
build_master_table.py
─────────────────────
One-shot converter that merges every match JSON in DATA_DIR into a single
Parquet file (one row per delivery) **without crashing on odd schemas**.

• Understands both modern  ➜  innings -> overs -> deliveries
  and older/flat          ➜  innings -> deliveries
  layouts automatically.

• Skips & logs innings that have *neither* key so you can inspect them later.
"""

from pathlib import Path
import json, polars as pl
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

# ── Paths ────────────────────────────────────────────────────────────────
DATA_DIR        = Path("/Users/arpitbhutani/Desktop/cricket/data")          # ↩ adjust if needed
OUT_PARQUET     = Path("/Users/arpitbhutani/Desktop/cricket/cricket_balls.parquet")
BAD_LOG_FILE    = Path("/Users/arpitbhutani/Desktop/cricket/missing_overs.log")

# ── Config ───────────────────────────────────────────────────────────────
BALLS_PER_OVER  = 6        # safest default – tweak if you ingest 8-ball comps

rows          = []
bad_files     = defaultdict(list)   # filename  ->  [innings numbers]

for fp in tqdm(list(DATA_DIR.glob("*.json")), desc="Parsing matches"):
    with fp.open() as f:
        match = json.load(f)

    info      = match["info"]
    match_id  = fp.stem
    match_dt  = datetime.fromisoformat(info["dates"][0]).date()

    for inn_no, inn in enumerate(match["innings"], start=1):
        # 1️⃣  Locate the list of deliveries ------------------------------
        if "overs" in inn:                                    # modern schema
            over_blocks = inn["overs"]
        elif "deliveries" in inn:                             # flat schema
            over_blocks = [{"over": None, "deliveries": inn["deliveries"]}]
        else:                                                 # totally unknown
            bad_files[fp.name].append(inn_no)
            continue

        batting = inn["team"]
        bowling = next(t for t in info["teams"] if t != batting)
        ball_counter = 0                                      # absolute within innings

        for ov in over_blocks:
            raw_over_no = ov.get("over")                      # may be None
            for idx_in_block, ball in enumerate(ov["deliveries"], start=1):
                ball_counter += 1

                # Derive over / ball-in-over robustly
                if raw_over_no is None:                       # flat schema
                    over_no = (ball_counter - 1) // BALLS_PER_OVER
                    ball_in_over = (ball_counter - 1) % BALLS_PER_OVER + 1
                else:
                    over_no = raw_over_no
                    ball_in_over = idx_in_block

                # ---- Build the row dict --------------------------------
                d = {
                    # Match meta -------------------------------------------------
                    "match_id":        match_id,
                    "match_date":      match_dt,
                    "event_name":      info.get("event", {}).get("name"),
                    "season":          info.get("season"),
                    "match_type":      info["match_type"],
                    "venue":           info["venue"],
                    "city":            info.get("city"),

                    # Innings context -------------------------------------------
                    "innings_number":  inn_no,
                    "batting_team":    batting,
                    "bowling_team":    bowling,

                    # Ball position ---------------------------------------------
                    "over":            over_no,
                    "ball_in_over":    ball_in_over,
                    "ball_number_absolute": ball_counter,

                    # Players ----------------------------------------------------
                    "batter":          ball["batter"],
                    "bowler":          ball["bowler"],
                    "non_striker":     ball["non_striker"],

                    # Runs / extras ---------------------------------------------
                    "runs_batter":     ball["runs"]["batter"],
                    "runs_extras":     ball["runs"]["extras"],
                    "runs_total":      ball["runs"]["total"],
                    "extras_type":     next(iter(ball.get("extras", {})), None),

                    # Boundaries & flags ----------------------------------------
                    "is_boundary_4":   ball["runs"]["batter"] == 4,
                    "is_boundary_6":   ball["runs"]["batter"] == 6,

                    # Wicket defaults -------------------------------------------
                    "wicket_type":     None,
                    "player_out":      None,
                    "fielders_involved": None,
                }

                # Optional wicket block -----------------------------------------
                if "wickets" in ball and ball["wickets"]:
                    w  = ball["wickets"][0]
                    d["wicket_type"] = w["kind"]
                    d["player_out"]  = w["player_out"]

                    fld_raw = w.get("fielders", [])
                    fld_norm = [
                        f["name"] if isinstance(f, dict) and "name" in f else str(f)
                        for f in fld_raw
                    ]
                    d["fielders_involved"] = ", ".join(fld_norm) or None

                rows.append(d)

# ── Persist the main table ───────────────────────────────────────────────
if rows:
    pl.DataFrame(rows).write_parquet(OUT_PARQUET)
    print(f"✅  Saved {OUT_PARQUET}  ({len(rows):,} rows)")
else:
    print("❌  No rows parsed – nothing written.")

# ── Report any innings we skipped ────────────────────────────────────────
if bad_files:
    with BAD_LOG_FILE.open("w") as log:
        for fname, inns in bad_files.items():
            log.write(f"{fname}\tmissing deliveries in innings {inns}\n")

    print(f"⚠️   {len(bad_files)} file(s) had no 'overs' or 'deliveries' key.")
    print(f"    Logged details to {BAD_LOG_FILE}")
else:
    print("🎉  All files conformed – no structural issues recorded.")
