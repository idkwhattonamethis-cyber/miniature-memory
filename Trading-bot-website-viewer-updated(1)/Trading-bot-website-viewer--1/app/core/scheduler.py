# background worker — seeds backtest on startup, ticks paper engine every 60s, compacts old data
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from app.core import db, paper, registry, ws

log = logging.getLogger("scheduler")

START_DATE = os.environ.get("SEED_START_DATE", "2026-01-01")
INITIAL_CAPITAL = float(os.environ.get("SEED_INITIAL_CAPITAL", "100000"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
# NOTE: REFRESH_WINDOW_DAYS below is now a fallback only — the real value
# comes from each bot's timeframe config (app/core/timeframe.py) via
# registry.get_timeframe_config(bot_id), so a bot with a different native
# cadence (hourly, 5-min...) doesn't have its refresh window forced to 30d.
REFRESH_WINDOW_DAYS = int(os.environ.get("REFRESH_WINDOW_DAYS", "30"))
# Seeds are baked into the image at seeds/ — deliberately NOT under data/, which is
# the persistent-disk mount on Render. A mount shadows any files committed under it,
# so keeping seeds out of data/ guarantees they're always readable for an instant
# cold start, while the SQLite DB stays on the disk so live data survives restarts.
SEED_DIR = Path(__file__).resolve().parents[2] / "seeds"
_LEGACY_SEED_DIR = Path(__file__).resolve().parents[2] / "data"

_stop = threading.Event()


# ---------------- seeding ----------------

def _load_seed_file(bot_id: str) -> bool:
    # load pre-baked backtest from committed JSON — instant cold start
    path = SEED_DIR / f"seed_{bot_id}.json"
    if not path.exists():
        path = _LEGACY_SEED_DIR / f"seed_{bot_id}.json"   # pre-move fallback
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        log.warning("Bad seed file for %s: %s", bot_id, e)
        return False

    equity = data.get("equity", {})
    strat_pts = equity.get("strategy", [])
    spy_pts = equity.get("spy", [])
    if len(strat_pts) < 10:
        return False

    db.replace_backtest_series(bot_id, {
        "strategy": [(ts, v) for ts, v in strat_pts],
        "spy": [(ts, v) for ts, v in spy_pts],
    })

    allocations = data.get("allocations", [])
    if allocations:
        alloc_rows = [{"date": a["date"], "data": json.dumps(a)} for a in allocations]
        db.save_daily_allocations(bot_id, alloc_rows)

    regime = data.get("regime", "")
    if regime:
        db.set_regime(bot_id, datetime.utcnow().date().isoformat(), regime)

    log.info("Loaded seed file for %s (%d strategy pts, %d allocations).",
             bot_id, len(strat_pts), len(allocations))
    return True


def seed_bot(bot_id: str) -> None:
    strat = registry.get_strategy(bot_id)
    try:
        points = strat.run_backtest(START_DATE, INITIAL_CAPITAL)
    except Exception as e:
        log.warning("Seed failed for %s: %s", bot_id, e)
        return

    strat_pts = points.get("strategy", [])
    if len(strat_pts) < 10 or any(v <= 0 for _, v in strat_pts):
        log.warning("Seed for %s produced invalid data; keeping existing.", bot_id)
        return

    go_live = db.get_meta(f"{bot_id}:go_live")
    if go_live:
        points = {
            series: [(ts, v) for (ts, v) in pts if ts[:10] < go_live]
            for series, pts in points.items()
        }

    allocations = points.pop("allocations", [])
    if allocations:
        alloc_rows = [
            {"date": a["date"], "data": json.dumps(a)}
            for a in allocations
        ]
        db.save_daily_allocations(bot_id, alloc_rows)
        log.info("Saved %d daily allocation snapshots for %s.", len(alloc_rows), bot_id)

    if db.has_backtest(bot_id):
        window_days = registry.get_timeframe_config(bot_id)["refresh_window_days"]
        cutoff = (datetime.utcnow().date() - timedelta(days=window_days)).isoformat()
        window = {
            series: [(ts, v) for (ts, v) in pts if ts[:10] >= cutoff]
            for series, pts in points.items()
        }
        n = db.refresh_backtest_window(bot_id, cutoff, window)
        log.info("Refreshed last %dd of %s (%d pts).", window_days, bot_id, n)
    else:
        db.replace_backtest_series(bot_id, points)
        log.info("Full seed %s (%d strategy pts).", bot_id, len(strat_pts))

    try:
        regime = strat.latest_regime()
        db.set_regime(bot_id, datetime.utcnow().date().isoformat(), regime)
    except Exception as e:
        log.warning("Regime fetch failed for %s: %s", bot_id, e)


def seed_all() -> None:
    for bot_id in registry.bot_ids():
        if db.has_backtest(bot_id):
            _refresh_recent(bot_id)
        else:
            seed_bot(bot_id)
    db.set_meta("last_seed", datetime.utcnow().isoformat())


def _splice_scale(existing: Optional[float], pts: List[tuple], anchor_n: int) -> float:
    # scale factor to align a freshly-refreshed window onto existing history.
    # Uses the MEDIAN ratio across the first `anchor_n` overlapping points
    # instead of a single point, so one noisy/stale tick can't throw off the
    # whole window (the old behavior anchored on a single point, which could
    # introduce a visible jump right at the refresh cutoff).
    if not existing or not pts:
        return 1.0
    sample = [v for _, v in pts[:max(anchor_n, 1)] if v > 0]
    if not sample:
        return 1.0
    ratios = sorted(existing / v for v in sample)
    mid = len(ratios) // 2
    return ratios[mid] if len(ratios) % 2 else (ratios[mid - 1] + ratios[mid]) / 2


def _refresh_recent(bot_id: str) -> None:
    # update the bot's recent history with fresh intraday-resolution prices,
    # using its own timeframe config (app/core/timeframe.py) instead of one
    # global window/resolution shared across every bot
    strat = registry.get_strategy(bot_id)
    cfg = registry.get_timeframe_config(bot_id)
    window_days = cfg["refresh_window_days"]
    anchor_n = cfg["splice_anchor_points"]
    try:
        cutoff = (datetime.utcnow().date() - timedelta(days=window_days)).isoformat()
        points = strat.run_backtest(cutoff, INITIAL_CAPITAL, hourly_window_days=window_days + 5)
    except Exception as e:
        log.warning("Recent refresh failed for %s: %s", bot_id, e)
        return

    strat_pts = points.get("strategy", [])
    if len(strat_pts) < 2:
        return

    # rescale to match existing history at the cutoff, using a median-anchor
    # splice (see _splice_scale) instead of a single-point anchor
    existing = db.last_value_before(bot_id, "strategy", cutoff)
    scale = _splice_scale(existing, strat_pts, anchor_n)
    if scale != 1.0:
        points["strategy"] = [(ts, v * scale) for ts, v in points["strategy"]]
    existing_spy = db.last_value_before(bot_id, "spy", cutoff)
    spy_pts = points.get("spy", [])
    scale_spy = _splice_scale(existing_spy, spy_pts, anchor_n)
    if scale_spy != 1.0:
        points["spy"] = [(ts, v * scale_spy) for ts, v in points["spy"]]

    # don't overwrite dates that already have live data
    live_dates = db.live_dates(bot_id)
    for series in list(points.keys()):
        if series == "allocations":
            continue
        points[series] = [(ts, v) for ts, v in points[series] if ts[:10] not in live_dates]

    allocations = points.pop("allocations", [])
    if allocations:
        alloc_rows = [{"date": a["date"], "data": json.dumps(a)} for a in allocations]
        db.save_daily_allocations(bot_id, alloc_rows)

    n = db.refresh_backtest_window(bot_id, cutoff, points)
    log.info("Refreshed last %dd of %s (%d pts, skipped %d live dates).",
              window_days, bot_id, n, len(live_dates))

    try:
        regime = strat.latest_regime()
        db.set_regime(bot_id, datetime.utcnow().date().isoformat(), regime)
    except Exception as e:
        log.warning("Regime fetch failed for %s: %s", bot_id, e)


def seed_missing() -> None:
    for bot_id in registry.bot_ids():
        if not db.has_backtest(bot_id):
            # try pre-baked seed file first — no downloads, instant data
            if not _load_seed_file(bot_id):
                seed_bot(bot_id)


# ---------------- live engine ----------------

def tick_all() -> None:
    for bot_id in registry.bot_ids():
        try:
            paper.tick(bot_id)
        except Exception as e:
            log.warning("Paper tick failed for %s: %s", bot_id, e)
    ws.notify("tick")


def maybe_compact() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if db.get_meta("last_compact") == today:
        return
    try:
        deleted = db.compact_live_to_hourly(today)
        db.set_meta("last_compact", today)
        log.info("Compacted %d intraday rows to hourly.", deleted)
    except Exception as e:
        log.warning("Compaction failed: %s", e)


# ---------------- loops ----------------

def _poll_loop() -> None:
    while not _stop.is_set():
        try:
            tick_all()
            maybe_compact()
        except Exception as e:
            log.exception("poll loop error: %s", e)
        _stop.wait(POLL_SECONDS)


def _seed_loop() -> None:
    while not _stop.wait(24 * 3600):
        try:
            seed_all()
        except Exception as e:
            log.exception("seed loop error: %s", e)


def _deferred_refresh() -> None:
    # wait for seed/daily downloads to finish and Yahoo rate limits to cool down
    _stop.wait(90)
    if _stop.is_set():
        return
    log.info("Starting deferred refresh for hourly data...")
    for bot_id in registry.bot_ids():
        if db.has_backtest(bot_id):
            try:
                _refresh_recent(bot_id)
            except Exception as e:
                log.warning("Deferred refresh failed for %s: %s", bot_id, e)
    log.info("Deferred refresh complete.")


def start() -> None:
    db.init_db()
    threading.Thread(target=seed_missing, daemon=True).start()
    threading.Thread(target=_poll_loop, daemon=True).start()
    threading.Thread(target=_deferred_refresh, daemon=True).start()
    log.info("Scheduler started (poll=%ss, start_date=%s)", POLL_SECONDS, START_DATE)


def stop() -> None:
    _stop.set()
