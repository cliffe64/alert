"""Extensible DEX data ingestion utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol

import httpx

from storage.sqlite_manager import upsert_bar, list_tokens

LOGGER = logging.getLogger(__name__)


class DexAdapter(Protocol):
    async def fetch_1m_bar(
        self,
        chain: str,
        token_address: str,
        pool_address: Optional[str],
        since_ts: Optional[int],
    ) -> Iterable[Dict[str, object]]:
        ...


@dataclass
class PancakeAdapter:
    """Simple adapter using Binance Smart Chain public API."""

    base_url: str = "https://api.pancakeswap.info/api/v2"

    async def fetch_1m_bar(
        self, chain: str, token_address: str, pool_address: Optional[str], since_ts: Optional[int]
    ) -> Iterable[Dict[str, object]]:
        del chain, pool_address, since_ts
        endpoint = f"/tokens/{token_address}"
        async with httpx.AsyncClient(base_url=self.base_url, timeout=10) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json().get("data", {})
        price = float(data.get("price", 0.0))
        if not price:
            return []
        return [
            {
                "open_ts": 0,
                "close_ts": 0,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume_base": 0.0,
                "volume_quote": 0.0,
                "notional_usd": 0.0,
                "trades": 0,
            }
        ]


_ADAPTERS: Dict[str, DexAdapter] = {
    "pancake": PancakeAdapter(),
}


def register_adapter(name: str, adapter: DexAdapter) -> None:
    _ADAPTERS[name] = adapter


async def fetch_1m_bar(
    chain: str, token_address: str, pool_address: Optional[str], since_ts: Optional[int]
) -> List[Dict[str, object]]:
    adapter = _ADAPTERS.get("pancake")
    if adapter is None:
        raise ValueError("No adapter registered for Pancake")
    bars = list(await adapter.fetch_1m_bar(chain, token_address, pool_address, since_ts))
    return bars


async def sync_registered_tokens(since_ts: Optional[int] = None) -> int:
    tokens = list_tokens(enabled=True)
    inserted = 0
    for token in tokens:
        adapter = _ADAPTERS.get(token["exchange"])
        if not adapter:
            LOGGER.debug("No adapter for %s", token["exchange"])
            continue
        try:
            bars = await adapter.fetch_1m_bar(
                token.get("chain", ""),
                token.get("token_address", ""),
                token.get("pool_address"),
                since_ts,
            )
        except Exception as exc:  # pragma: no cover - network errors
            LOGGER.exception("Failed to fetch DEX bars for %s: %s", token["symbol"], exc)
            continue
        for bar in bars:
            payload = {
                **bar,
                "source": "dex",
                "exchange": token["exchange"],
                "chain": token.get("chain", ""),
                "symbol": token["symbol"],
                "base": token.get("base", ""),
                "quote": token.get("quote", ""),
            }
            upsert_bar("bars_1m", payload)
            inserted += 1
    return inserted


__all__ = ["fetch_1m_bar", "register_adapter", "sync_registered_tokens", "PancakeAdapter"]
