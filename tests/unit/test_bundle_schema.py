from luna_mlb_analytics.ingestion.bundle_schema import validate_bundle


def test_validate_bundle_accepts_fixture(sample_bundle):
    validate_bundle(sample_bundle)


def test_validate_bundle_rejects_missing_games(sample_bundle):
    broken = dict(sample_bundle)
    broken.pop("games")
    try:
        validate_bundle(broken)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "missing" in str(exc).lower()
