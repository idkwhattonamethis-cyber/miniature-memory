# per-bot timeframe config — the single place that decides what resolution a
# bot's backtest and live data get stored/refreshed at. A bot declares a
# `timeframe` string in its METADATA (e.g. "1d", "1h"); everything else
# (scheduler refresh windows, intraday fill resolution, live poll cadence)
# derives from this table instead of being hardcoded per-bot in scheduler.py.
#
# Adding a bot with a different native cadence (e.g. an hourly-rebalance
# strategy) means adding a row here + setting METADATA["timeframe"] — no
# changes to scheduler.py/db.py required.
from __future__ import annotations

from typing import TypedDict


class TimeframeConfig(TypedDict):
    # how far back "recent" backtest history gets refreshed with intraday
    # fill (older history stays at the bot's coarse/authoritative bars)
    refresh_window_days: int
    # bar resolution used to fill in intraday detail within the refresh window
    intraday_fill: str
    # how often the live paper engine marks this bot to market
    live_poll_seconds: int
    # how many overlapping points to use when splicing a refreshed backtest
    # window onto existing history, to avoid single-point-anchor discontinuities
    splice_anchor_points: int


_TIMEFRAMES: dict[str, TimeframeConfig] = {
    # daily rebalance — long daily history is authoritative, last ~30d get
    # hourly intraday detail so the chart isn't a staircase near "today"
    "1d": {
        "refresh_window_days": 30,
        "intraday_fill": "1h",
        "live_poll_seconds": 60,
        "splice_anchor_points": 5,
    },
    # hourly rebalance — shorter refresh window since hourly bars are already
    # fine-grained; no need for a separate intraday fill pass
    "1h": {
        "refresh_window_days": 7,
        "intraday_fill": "1h",
        "live_poll_seconds": 60,
        "splice_anchor_points": 3,
    },
    # fast/intraday strategies — refresh window measured in a couple of days,
    # minute-level fill, still mark-to-market every 60s (Yahoo's finest
    # reliable free-tier granularity)
    "5m": {
        "refresh_window_days": 2,
        "intraday_fill": "5m",
        "live_poll_seconds": 60,
        "splice_anchor_points": 3,
    },
}

DEFAULT_TIMEFRAME = "1d"


def config_for(timeframe: str | None) -> TimeframeConfig:
    return _TIMEFRAMES.get(timeframe or DEFAULT_TIMEFRAME, _TIMEFRAMES[DEFAULT_TIMEFRAME])
