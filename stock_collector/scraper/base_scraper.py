from dataclasses import dataclass


@dataclass
class ScrapeResult:
    symbol: str
    trade_date: str
    payload: dict
