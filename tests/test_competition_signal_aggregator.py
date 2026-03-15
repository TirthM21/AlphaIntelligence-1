from pathlib import Path

from scripts.competition.generate_daily_competition_list import aggregate, parse_int_arg, parse_report


def test_parse_report_extracts_buy_and_sell_rows(tmp_path: Path):
    report = tmp_path / "scan.txt"
    report.write_text(
        "\n".join(
            [
                "🟢 BUY #1: RELIANCE | Score: 92.5/110",
                "🟢 BUY #2: INFY | Score: 88.0/110",
                "🔴 SELL #1: XYZ | Score: 77.0/110",
            ]
        ),
        encoding="utf-8",
    )

    buys, sells = parse_report(report)

    assert [b.ticker for b in buys] == ["RELIANCE", "INFY"]
    assert [round(b.score, 1) for b in buys] == [92.5, 88.0]
    assert [s.ticker for s in sells] == ["XYZ"]


def test_aggregate_builds_consensus_and_confidence():
    class Row:
        def __init__(self, ticker, score, side):
            self.ticker = ticker
            self.score = score
            self.side = side

    signals_by_strategy = {
        "daily_momentum": ([Row("RELIANCE", 90, "BUY"), Row("INFY", 85, "BUY")], [Row("ABC", 70, "SELL")]),
        "long_term": ([Row("RELIANCE", 80, "BUY")], [Row("ABC", 75, "SELL"), Row("XYZ", 65, "SELL")]),
    }

    condensed = aggregate(signals_by_strategy, top_n=5)

    assert condensed["buys"][0]["ticker"] == "RELIANCE"
    assert condensed["buys"][0]["votes"] == 2
    assert condensed["buys"][0]["confidence_pct"] == 100.0
    assert condensed["sells"][0]["ticker"] == "ABC"
    assert condensed["sells"][0]["votes"] == 2


def test_parse_int_arg_supports_commas_and_all_keyword():
    assert parse_int_arg("10,000", arg_name="--limit", allow_none=True) == 10000
    assert parse_int_arg("all", arg_name="--limit", allow_none=True) is None


def test_parse_int_arg_rejects_invalid_values():
    try:
        parse_int_arg("not-a-number", arg_name="--limit")
        assert False, "Expected parse_int_arg to raise"
    except Exception as exc:
        assert "integer" in str(exc)
