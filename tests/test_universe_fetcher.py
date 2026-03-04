from datetime import datetime

from src.data.universe_fetcher import StockUniverseFetcher


class DummyNSEFetcher:
    def get_all_equity_stocks(self):
        return ["RELIANCE", "TCS"]

    def get_etfs(self):
        return ["NIFTYBEES"]

    def get_index_stocks(self, _index_name):
        return []


def test_fetch_universe_equity_and_etf_cache_paths(monkeypatch, tmp_path):
    monkeypatch.setattr("src.data.universe_fetcher.NSEFetcher", DummyNSEFetcher)
    fetcher = StockUniverseFetcher(cache_dir=str(tmp_path))

    equity_symbols = fetcher.fetch_universe(include_etfs=False)
    etf_symbols = fetcher.fetch_universe(include_etfs=True)

    assert equity_symbols == ["RELIANCE.NS", "TCS.NS"]
    assert etf_symbols == ["NIFTYBEES.NS"]

    assert (tmp_path / "nse_equity_universe.pkl").exists()
    assert (tmp_path / "nse_etf_universe.pkl").exists()


def test_get_universe_info_uses_selected_universe_cache(monkeypatch, tmp_path):
    monkeypatch.setattr("src.data.universe_fetcher.NSEFetcher", DummyNSEFetcher)
    fetcher = StockUniverseFetcher(cache_dir=str(tmp_path))

    fetcher.fetch_universe(include_etfs=False)
    fetcher.fetch_universe(include_etfs=True)

    equity_info = fetcher.get_universe_info(include_etfs=False)
    etf_info = fetcher.get_universe_info(include_etfs=True)

    assert equity_info["cached"] is True
    assert etf_info["cached"] is True

    assert equity_info["count"] == 2
    assert etf_info["count"] == 1

    datetime.fromisoformat(equity_info["fetch_date"])
    datetime.fromisoformat(etf_info["fetch_date"])

    assert equity_info["cache_age_hours"] >= 0
    assert etf_info["cache_age_hours"] >= 0
