import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--ci", action="store_true", default=False, help="run CI-specific tests"
    )

def pytest_collection_modifyitems(config, items):
    if config.getoption("--ci"):
        return
    skip_slow = pytest.mark.skip(reason="need --ci option to run")
    for item in items:
        if "ci" in item.keywords:
            item.add_marker(skip_slow)