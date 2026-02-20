"""
SkyFit Churn Prediction — Database Configuration
==================================================
Loads PostgreSQL credentials from the SkyFit datalake .env file.

Supports two modes:
  1. Load from .env file: reads C:\\skyfit-datalake\\config\\.env
  2. Explicit --db-url: CLI override takes precedence

Usage:
  from config.database import get_connection_string
  conn_str = get_connection_string()

Security:
  - Credentials are NEVER hardcoded in source files
  - .env file is loaded at runtime only
  - Connection string uses SSL (sslmode=require)
"""

import os
import logging
from pathlib import Path
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

# Default path to the SkyFit datalake .env file
DEFAULT_ENV_PATH = Path(r"C:\skyfit-datalake\config\.env")


def _load_env_file(env_path: Path = None) -> None:
    """
    Load environment variables from .env file.

    Uses python-dotenv if available, falls back to manual parsing.
    """
    env_path = env_path or DEFAULT_ENV_PATH

    if not env_path.exists():
        logger.warning("Env file not found: %s", env_path)
        return

    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)
        logger.info("Loaded env from %s (python-dotenv)", env_path)
    except ImportError:
        # Fallback: manual parsing for environments without python-dotenv
        logger.info("python-dotenv not installed, parsing .env manually")
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    # Don't override existing env vars
                    if key not in os.environ:
                        os.environ[key] = value


def get_connection_string(env_path: Path = None) -> str:
    """
    Build PostgreSQL connection string from environment variables.

    Reads from .env file, then constructs:
      postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require

    Returns
    -------
    str
        SQLAlchemy-compatible connection string.

    Raises
    ------
    ValueError
        If required environment variables are missing.
    """
    _load_env_file(env_path)

    host = os.environ.get("PG_HOST")
    port = os.environ.get("PG_PORT", "5432")
    database = os.environ.get("PG_DATABASE")
    user = os.environ.get("PG_USER")
    password = os.environ.get("PG_PASSWORD")
    sslmode = os.environ.get("PG_SSLMODE", "require")

    missing = []
    if not host:
        missing.append("PG_HOST")
    if not database:
        missing.append("PG_DATABASE")
    if not user:
        missing.append("PG_USER")
    if not password:
        missing.append("PG_PASSWORD")

    if missing:
        raise ValueError(
            f"Missing required database environment variables: {missing}. "
            f"Set them in {DEFAULT_ENV_PATH} or as environment variables."
        )

    # URL-encode password (handles special characters like @, !)
    encoded_password = quote_plus(password)

    conn_str = (
        f"postgresql://{user}:{encoded_password}@{host}:{port}"
        f"/{database}?sslmode={sslmode}"
    )

    logger.info(
        "Database connection: %s@%s:%s/%s (sslmode=%s)",
        user, host, port, database, sslmode,
    )

    return conn_str
