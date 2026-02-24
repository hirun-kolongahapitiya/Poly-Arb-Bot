from __future__ import annotations
from dotenv import load_dotenv

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional
import logging
import base64
from nacl.signing import SigningKey
import requests

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# -------------------------------
# Market Snapshot
# -------------------------------
@dataclass
class MarketSnapshot:
    market_slug: str
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade_px: Optional[float] = None
    bids: list[dict] = field(default_factory=list)
    offers: list[dict] = field(default_factory=list)

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return self.last_trade_px


state: dict[str, MarketSnapshot] = {}


# -------------------------------
# Helpers
# -------------------------------
def _parse_px(obj: Any) -> Optional[float]:
    try:
        return float(obj)
    except (TypeError, ValueError):
        return None


def _parse_market_data(payload: dict) -> None:
    md = payload.get("marketData")
    if not md:
        return

    slug = md.get("marketSlug")
    if not slug:
        return

    bids = md.get("bids") or []
    offers = md.get("offers") or []

    best_bid = _parse_px(bids[0]["px"]) if bids else None
    best_ask = _parse_px(offers[0]["px"]) if offers else None

    stats = md.get("stats") or {}
    last_trade_px = _parse_px(stats.get("lastTradePx"))

    if slug not in state:
        state[slug] = MarketSnapshot(market_slug=slug)

    s = state[slug]
    s.best_bid = best_bid
    s.best_ask = best_ask
    s.last_trade_px = last_trade_px
    s.bids = bids
    s.offers = offers


# -------------------------------
# Fetch Active Markets
# -------------------------------
def get_active_btc_15m_market_slugs() -> list[str]:
    """
    Fetch all active BTC 15-minute markets
    """
    url = "https://api.polymarket.us/v1/markets"
    params = {
        "active": "true",
        "closed": "false",
        "archived": "false",
        "limit": 500
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        markets = response.json()
    except Exception as e:
        logger.error("Failed to fetch active markets: %s", e)
        return []

    # filter markets containing "btc" and "15m" in the slug
    slugs = [
        m["marketSlug"]
        for m in markets
        if m.get("marketSlug") and "btc" in m["marketSlug"].lower() and "15m" in m["marketSlug"].lower()
    ]

    logger.info("Fetched %d active BTC 15-min markets", len(slugs))
    return slugs


# -------------------------------
# Ed25519 Auth
# -------------------------------
logger = logging.getLogger(__name__)

def _ed25519_headers(path: str = "/v1/ws/markets") -> dict[str, str]:
    key_id = os.getenv("POLYMARKET_ACCESS_KEY", "").strip()
    secret = os.getenv("POLYMARKET_SECRET_KEY", "").strip()

    if not key_id or not secret:
        raise RuntimeError("Missing POLYMARKET_ACCESS_KEY or POLYMARKET_SECRET_KEY")

    # Check system time vs real world time from Polymarket API
    try:
        resp = requests.get("https://api.polymarket.us/v1/server-time", timeout=5)
        resp.raise_for_status()
        server_ts = int(resp.json().get("serverTime", 0))
    except Exception as e:
        logger.warning("Failed to fetch server time: %s", e)
        server_ts = None

    local_ts = int(time.time() * 1000)

    if server_ts:
        diff = abs(local_ts - server_ts)
        if diff > 5000:  # more than 5 seconds
            logger.warning(
                "Local clock differs from server by %d ms. This may cause timestamp errors.", diff
            )

    raw_key = base64.urlsafe_b64decode(secret)
    sk = SigningKey(raw_key[:32])

    ts = str(local_ts)  # use current local timestamp in ms
    message = ts + "GET" + path
    sig = sk.sign(message.encode()).signature
    sig_b64 = base64.urlsafe_b64encode(sig).decode()

    return {
        "X-PM-Access-Key": key_id,
        "X-PM-Timestamp": ts,
        "X-PM-Signature": sig_b64
    }

# -------------------------------
# WebSocket Runner
# -------------------------------
async def run_ws():
    import websockets

    url = "wss://api.polymarket.us/v1/ws/markets"
    headers = _ed25519_headers()
    headers["User-Agent"] = "Mozilla/5.0"

    print("HEADERS BEING SENT:")
    print(headers)

    while True:
        slugs = get_active_btc_15m_market_slugs()
        if not slugs:
            logger.warning("No active markets found. Retrying...")
            await asyncio.sleep(10)
            continue

        try:
            async with websockets.connect(
                url,
                extra_headers=headers,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                sub = {
                    "subscribe": {
                        "requestId": "1",
                        "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                        "marketSlugs": slugs,
                    }
                }

                await ws.send(json.dumps(sub))
                logger.info("Subscribed to %d markets", len(slugs))

                async for message in ws:
                    msg = json.loads(message)
                    if msg.get("subscriptionType") == "SUBSCRIPTION_TYPE_MARKET_DATA":
                        _parse_market_data(msg)

        except Exception as e:
            logger.error("WebSocket error: %s. Reconnecting...", e)
            await asyncio.sleep(5)


# -------------------------------
# Monitor Loop (example logging)
# -------------------------------
async def monitor():
    while True:
        for slug, snapshot in list(state.items())[:5]:  # only log first 5 to avoid spam
            logger.info(
                f"{slug} | bid={snapshot.best_bid} "
                f"ask={snapshot.best_ask} mid={snapshot.mid_price}"
            )
        await asyncio.sleep(5)


# -------------------------------
# MAIN ENTRY POINT
# -------------------------------
if __name__ == "__main__":

    async def main():
        await asyncio.gather(
            run_ws(),
            monitor(),
        )

    asyncio.run(main())