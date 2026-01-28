import os
from dataclasses import dataclass
from urllib.parse import quote_plus

from dotenv import load_dotenv


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().replace("\u00a0", " ")
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


@dataclass(frozen=True)
class Settings:
    api_timeout: float
    use_database: bool
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    force_reparse: bool
    parse_workers: int
    models_per_brand: int

    @property
    def database_url(self) -> str:
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        return (
            f"postgresql+psycopg2://{user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def database_url_for(self, db_name: str) -> str:
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        return (
            f"postgresql+psycopg2://{user}:{password}"
            f"@{self.db_host}:{self.db_port}/{db_name}"
        )


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        api_timeout=_env_float("API_TIMEOUT", 3.0),
        use_database=_env_bool("USE_DATABASE", True),
        db_host=_env_str("DB_HOST", "localhost"),
        db_port=_env_int("DB_PORT", 5432),
        db_name=_env_str("DB_NAME", "cars"),
        db_user=_env_str("DB_USER", "postgres"),
        db_password=_env_str("DB_PASSWORD", "postgres"),
        force_reparse=_env_bool("FORCE_REPARSE", True),
        parse_workers=_env_int("PARSE_WORKERS", 10),
        models_per_brand=_env_int("MODELS_PER_BRAND", 0),
    )
