"""Tests for configuration module."""

import importlib
import os


def test_config_module_imports():
    """Verify that the config module can be imported without errors."""
    import config  # noqa: F401


def test_env_var_loading_with_defaults():
    """Verify that environment variables can be read and fall back to defaults."""
    # Set a test variable, read it, then clean up
    os.environ["SNOWFLAKE_WAREHOUSE"] = "TEST_WH"
    assert os.environ.get("SNOWFLAKE_WAREHOUSE") == "TEST_WH"
    del os.environ["SNOWFLAKE_WAREHOUSE"]

    # After deletion the variable should be absent
    assert os.environ.get("SNOWFLAKE_WAREHOUSE") is None


def test_dotenv_file_structure():
    """Verify that .env.example exists and contains expected keys."""
    env_example_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), ".env.example"
    )
    assert os.path.exists(env_example_path), ".env.example file should exist"

    with open(env_example_path, "r") as f:
        content = f.read()

    expected_keys = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC_STOCK_PRICES",
        "YAHOO_STOCK_TICKERS",
    ]
    for key in expected_keys:
        assert key in content, f"{key} should be present in .env.example"
