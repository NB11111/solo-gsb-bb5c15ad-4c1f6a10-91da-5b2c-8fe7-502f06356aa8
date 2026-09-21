"""Pytest fixtures: fresh event store per test, ASGI transport client."""

import pytest
from fastapi.testclient import TestClient

from app import main as main_module


@pytest.fixture()
def client() -> TestClient:
    main_module.store.reset()
    with TestClient(main_module.app) as c:
        yield c
    main_module.store.reset()
