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


def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = os.getenv(name)
    if raw is None:
        return default
    result = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                result.append(int(part))
            except ValueError:
                pass
    return result if result else default


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
    pagesize: int
    img_path: str
    parse_photos: bool
    download_photos: bool
    max_photo_combinations: int
    max_colors: int
    parse_panoramas: bool
    download_panoramas: bool
    parse_modes: list[int]
    photo_360_only: bool
    photo_360_only_categories: list[int]
    log_dir: str
    log_retention_days: int

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
        pagesize=_env_int("PAGESIZE", 10),
        img_path=_env_str("IMG_PATH", "IMG"),
        parse_photos=_env_bool("PARSE_PHOTOS", True),
        download_photos=_env_bool("DOWNLOAD_PHOTOS", True),
        max_photo_combinations=_env_int("MAX_PHOTO_COMBINATIONS", 0),
        max_colors=_env_int("MAX_COLORS", 0),
        parse_panoramas=_env_bool("PARSE_PANORAMAS", True),
        download_panoramas=_env_bool("DOWNLOAD_PANORAMAS", True),
        parse_modes=_env_int_list("PARSE_MODES", [1]),
        photo_360_only=_env_bool("360ONLY", False),
        photo_360_only_categories=_env_int_list("360ONLYIDPHOTO", [1]),
        log_dir=_env_str("LOG_DIR", "logs"),
        log_retention_days=_env_int("LOG_RETENTION_DAYS", 7),
    )
