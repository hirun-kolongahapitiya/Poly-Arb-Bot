from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
POLL_SECONDS = 5
MARKET_REFRESH_SECONDS = 120
MAX_MARKETS = 6
MARKET_FILTER = os.getenv("MARKET_FILTER", "btc").strip().lower()
TARGET_MARKET_SLUG = os.getenv("TARGET_MARKET_SLUG", "").strip().lower()
SLUG_STEP_SECONDS = int(os.getenv("SLUG_STEP_SECONDS", "900").strip() or "900")
ORDERBOOK_LEVELS = int(os.getenv("ORDERBOOK_LEVELS", "4").strip() or "4")
SHOW_ORDERBOOK_LEVELS = os.getenv("SHOW_ORDERBOOK_LEVELS", "1").strip() != "0"


@dataclass
class TokenConfig:
    market_slug: str
    question: str
    outcome: str
    token_id: str


@dataclass
class TokenSnapshot:
    market_slug: str
    question: str
    outcome: str
    token_id: str
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    best_bid_shares: Optional[float] = None
    best_ask_shares: Optional[float] = None
    mid_price: Optional[float] = None
    last_trade_price: Optional[float] = None
    bid_levels: int = 0
    ask_levels: int = 0
    timestamp_ms: Optional[int] = None
    top_bids: list[dict[str, float]] = field(default_factory=list)
    top_asks: list[dict[str, float]] = field(default_factory=list)


state: dict[str, TokenSnapshot] = {}
tracked_token_ids: list[str] = []


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_book_side(levels: list[dict[str, Any]], is_bid: bool) -> list[dict[str, float]]:
    parsed: list[dict[str, float]] = []
    for level in levels:
        price = _parse_float(level.get("price"))
        shares = _parse_float(level.get("size"))
        if price is None or shares is None:
            continue
        parsed.append({"price": price, "shares": shares})

    parsed.sort(key=lambda lvl: lvl["price"], reverse=is_bid)

    cumulative_total = 0.0
    for level in parsed:
        total = level["price"] * level["shares"]
        cumulative_total += total
        level["total"] = total
        level["cum_total"] = cumulative_total
    return parsed


def _fmt_num(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "None"
    return f"{value:.{decimals}f}"


def _check_credentials() -> None:
    if os.getenv("POLYMARKET_ACCESS_KEY", "").strip() and (
        os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        or os.getenv("POLYMARKET_SECRET_KEY", "").strip()
    ):
        logger.info("Loaded Polymarket API key credentials from .env")
        return

    required = [
        "WALLET_ADDRESS",
        "POLY_API_KEY",
        "POLY_API_SECRET",
        "POLY_API_PASSPHRASE",
        "PK",
    ]
    present = [key for key in required if os.getenv(key, "").strip()]
    if len(present) == len(required):
        logger.info("Loaded API credentials from .env")
    elif present:
        logger.info(
            "Partial credentials in .env (%d/%d keys). Running read-only market data mode.",
            len(present),
            len(required),
        )
    else:
        logger.info("No trading credentials found. Running read-only market data mode.")


def _market_matches_filter(question: str, slug: str) -> bool:
    if TARGET_MARKET_SLUG:
        return slug.lower() in _rolling_slug_candidates(TARGET_MARKET_SLUG, SLUG_STEP_SECONDS)

    if not MARKET_FILTER:
        return True
    haystack = f"{question} {slug}".lower()
    return MARKET_FILTER in haystack


def _rolling_slug_candidates(base_slug: str, step_seconds: int) -> list[str]:
    if not base_slug:
        return []

    match = re.match(r"^(.*)-(\d{9,})$", base_slug)
    if not match:
        return [base_slug]

    prefix, ts_raw = match.groups()
    base_ts = int(ts_raw)
    step = max(1, step_seconds)
    now_ts = int(time.time())

    if now_ts <= base_ts:
        aligned_ts = base_ts
    else:
        aligned_ts = base_ts + ((now_ts - base_ts) // step) * step

    ordered = [
        f"{prefix}-{aligned_ts}",
        f"{prefix}-{aligned_ts + step}",
        f"{prefix}-{max(0, aligned_ts - step)}",
        base_slug,
    ]

    # Deduplicate while preserving priority order.
    return list(dict.fromkeys(ordered))


def _markets_for_target_slug() -> list[dict[str, Any]]:
    candidates = _rolling_slug_candidates(TARGET_MARKET_SLUG, SLUG_STEP_SECONDS)

    for candidate in candidates:
        response = requests.get(
            f"{GAMMA_BASE}/markets",
            params={"slug": candidate, "limit": 5},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            continue

        for market in payload:
            slug = (market.get("slug") or "").strip().lower()
            if not slug:
                continue
            if market.get("active") is not True or market.get("closed") is True:
                continue
            # Return only one live market to avoid mixing current/next intervals.
            return [market]

    return []


def fetch_active_tokens(max_markets: int = MAX_MARKETS) -> list[TokenConfig]:
    if TARGET_MARKET_SLUG:
        markets = _markets_for_target_slug()
    else:
        params = {"active": "true", "closed": "false", "limit": 100}
        response = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=20)
        response.raise_for_status()
        markets = response.json()

    selected: list[TokenConfig] = []

    for market in markets:
        question = market.get("question", "") or ""
        slug = market.get("slug", "") or ""
        if not _market_matches_filter(question, slug):
            continue

        token_ids = _parse_json_list(market.get("clobTokenIds"))
        outcomes = _parse_json_list(market.get("outcomes"))
        if not token_ids:
            continue

        if not outcomes:
            outcomes = [f"outcome-{idx + 1}" for idx in range(len(token_ids))]

        for idx, token_id in enumerate(token_ids):
            outcome = outcomes[idx] if idx < len(outcomes) else f"outcome-{idx + 1}"
            selected.append(
                TokenConfig(
                    market_slug=slug or f"market-{market.get('id', 'unknown')}",
                    question=question,
                    outcome=outcome,
                    token_id=str(token_id),
                )
            )

        if len({t.market_slug for t in selected}) >= max_markets:
            break

    if not selected and TARGET_MARKET_SLUG:
        candidates = ", ".join(_rolling_slug_candidates(TARGET_MARKET_SLUG, SLUG_STEP_SECONDS))
        logger.warning("No active market found for rolling slug candidates: %s", candidates)
        return []

    if not selected and markets:
        for market in markets[:max_markets]:
            question = market.get("question", "") or ""
            slug = market.get("slug", "") or f"market-{market.get('id', 'unknown')}"
            token_ids = _parse_json_list(market.get("clobTokenIds"))
            outcomes = _parse_json_list(market.get("outcomes"))
            for idx, token_id in enumerate(token_ids):
                outcome = outcomes[idx] if idx < len(outcomes) else f"outcome-{idx + 1}"
                selected.append(
                    TokenConfig(
                        market_slug=slug,
                        question=question,
                        outcome=outcome,
                        token_id=str(token_id),
                    )
                )

    return selected


def fetch_token_snapshot(config: TokenConfig) -> tuple[Optional[TokenSnapshot], Optional[str]]:
    try:
        book_response = requests.get(
            f"{CLOB_BASE}/book",
            params={"token_id": config.token_id},
            timeout=15,
        )
        book_response.raise_for_status()
        book = book_response.json()

        last_trade_response = requests.get(
            f"{CLOB_BASE}/last-trade-price",
            params={"token_id": config.token_id},
            timeout=15,
        )
        last_trade_response.raise_for_status()
        last_trade = last_trade_response.json()
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 404:
            logger.warning(
                "Orderbook unavailable (404) for token %s (%s).",
                config.token_id,
                config.market_slug,
            )
            return None, config.token_id
        logger.warning(
            "Orderbook fetch failed for token %s (%s): %s",
            config.token_id,
            config.market_slug,
            exc,
        )
        return None, None
    except Exception as exc:
        logger.warning(
            "Orderbook fetch failed for token %s (%s): %s",
            config.token_id,
            config.market_slug,
            exc,
        )
        return None, None

    parsed_bids = _parse_book_side(book.get("bids", []) or [], is_bid=True)
    parsed_asks = _parse_book_side(book.get("asks", []) or [], is_bid=False)

    best_bid = parsed_bids[0]["price"] if parsed_bids else None
    best_ask = parsed_asks[0]["price"] if parsed_asks else None
    best_bid_shares = parsed_bids[0]["shares"] if parsed_bids else None
    best_ask_shares = parsed_asks[0]["shares"] if parsed_asks else None

    mid = None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0

    return (
        TokenSnapshot(
            market_slug=config.market_slug,
            question=config.question,
            outcome=config.outcome,
            token_id=config.token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_shares=best_bid_shares,
            best_ask_shares=best_ask_shares,
            mid_price=mid,
            last_trade_price=_parse_float(last_trade.get("price")),
            bid_levels=len(parsed_bids),
            ask_levels=len(parsed_asks),
            timestamp_ms=int(book.get("timestamp")) if str(book.get("timestamp", "")).isdigit() else None,
            top_bids=parsed_bids[:ORDERBOOK_LEVELS],
            top_asks=parsed_asks[:ORDERBOOK_LEVELS],
        ),
        None,
    )


async def run_collector() -> None:
    _check_credentials()
    if TARGET_MARKET_SLUG:
        logger.info(
            "Rolling slug mode enabled: base=%s step=%ss",
            TARGET_MARKET_SLUG,
            SLUG_STEP_SECONDS,
        )
    token_configs: list[TokenConfig] = []
    last_refresh = 0.0

    while True:
        now = asyncio.get_running_loop().time()
        if not token_configs or (now - last_refresh) >= MARKET_REFRESH_SECONDS:
            try:
                token_configs = await asyncio.to_thread(fetch_active_tokens, MAX_MARKETS)
                last_refresh = now
                tracked_token_ids[:] = [token.token_id for token in token_configs]

                # Drop snapshots that are no longer part of the currently tracked market.
                tracked_set = set(tracked_token_ids)
                stale_ids = [token_id for token_id in list(state.keys()) if token_id not in tracked_set]
                for token_id in stale_ids:
                    state.pop(token_id, None)

                scope = (
                    f"rolling_slug={TARGET_MARKET_SLUG}"
                    if TARGET_MARKET_SLUG
                    else f"filter={MARKET_FILTER!r}"
                )
                logger.info(
                    "Tracking %d token outcomes across %d markets (%s)",
                    len(token_configs),
                    len({t.market_slug for t in token_configs}),
                    scope,
                )
            except Exception as exc:
                logger.error("Failed to refresh active markets: %s", exc)
                await asyncio.sleep(POLL_SECONDS)
                continue

        if not token_configs:
            logger.warning("No active tokens available. Retrying...")
            await asyncio.sleep(POLL_SECONDS)
            continue

        tasks = [asyncio.to_thread(fetch_token_snapshot, token) for token in token_configs]
        results = await asyncio.gather(*tasks)
        unavailable_token_ids: set[str] = set()

        for snapshot, unavailable_token_id in results:
            if snapshot:
                state[snapshot.token_id] = snapshot
            if unavailable_token_id:
                unavailable_token_ids.add(unavailable_token_id)

        if unavailable_token_ids:
            token_configs = [
                token for token in token_configs if token.token_id not in unavailable_token_ids
            ]
            tracked_token_ids[:] = [token.token_id for token in token_configs]
            for token_id in unavailable_token_ids:
                state.pop(token_id, None)
            logger.info(
                "Temporarily skipped %d unavailable token(s) until next market refresh.",
                len(unavailable_token_ids),
            )

        await asyncio.sleep(POLL_SECONDS)


async def monitor() -> None:
    while True:
        snapshots = [state[token_id] for token_id in tracked_token_ids if token_id in state][:8]
        if snapshots:
            for snapshot in snapshots:
                bid_total = (
                    snapshot.best_bid * snapshot.best_bid_shares
                    if snapshot.best_bid is not None and snapshot.best_bid_shares is not None
                    else None
                )
                ask_total = (
                    snapshot.best_ask * snapshot.best_ask_shares
                    if snapshot.best_ask is not None and snapshot.best_ask_shares is not None
                    else None
                )
                logger.info(
                    "%s [%s] | bid=%s sh=%s total=%s | ask=%s sh=%s total=%s | mid=%s last=%s | depth(b/a)=%d/%d",
                    snapshot.market_slug,
                    snapshot.outcome,
                    _fmt_num(snapshot.best_bid),
                    _fmt_num(snapshot.best_bid_shares),
                    _fmt_num(bid_total),
                    _fmt_num(snapshot.best_ask),
                    _fmt_num(snapshot.best_ask_shares),
                    _fmt_num(ask_total),
                    _fmt_num(snapshot.mid_price, 3),
                    _fmt_num(snapshot.last_trade_price),
                    snapshot.bid_levels,
                    snapshot.ask_levels,
                )
                if SHOW_ORDERBOOK_LEVELS:
                    bid_ladder = ", ".join(
                        f"{level['price']:.2f}:{level['shares']:.2f}(${level['total']:.2f})"
                        for level in snapshot.top_bids
                    )
                    ask_ladder = ", ".join(
                        f"{level['price']:.2f}:{level['shares']:.2f}(${level['total']:.2f})"
                        for level in snapshot.top_asks
                    )
                    logger.info("  bids[%d]: %s", ORDERBOOK_LEVELS, bid_ladder or "n/a")
                    logger.info("  asks[%d]: %s", ORDERBOOK_LEVELS, ask_ladder or "n/a")
        else:
            logger.info("Waiting for first orderbook snapshots...")
        await asyncio.sleep(POLL_SECONDS)


async def main() -> None:
    await asyncio.gather(run_collector(), monitor())


if __name__ == "__main__":
    asyncio.run(main())
