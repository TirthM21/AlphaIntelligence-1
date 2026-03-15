from src.strategies.method_catalog import get_strategy_method_catalogue, list_strategy_tracks


def test_strategy_method_catalog_contains_value_and_algorithmic_tracks():
    catalogue = get_strategy_method_catalogue()

    assert set(catalogue.keys()) == {"value_investing", "algorithmic"}
    assert len(catalogue["value_investing"]) >= 3
    assert len(catalogue["algorithmic"]) >= 5


def test_strategy_tracks_list_is_sorted_and_stable():
    tracks = list_strategy_tracks()

    assert tracks == sorted(tracks)
    assert tracks == ["algorithmic", "value_investing"]
