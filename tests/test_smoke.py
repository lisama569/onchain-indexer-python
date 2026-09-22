"""Trivial smoke test verifying the package imports and app builds."""


def test_app_builds():
    from app.api import create_app
    from app.config import Settings

    app = create_app(settings=Settings(database_url="sqlite:///:memory:"))
    assert app.title == "onchain-indexer"
