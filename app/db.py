import os
import re
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import Settings


def create_db_engine(settings: Settings):
    import logging
    
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    
    # Полностью отключаем логирование SQLAlchemy
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy.dialects").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy.orm").setLevel(logging.ERROR)
    
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": int(settings.api_timeout), "client_encoding": "UTF8"},
        echo=False,  # Отключаем вывод SQL запросов
        future=True,
    )


def ensure_database_exists(settings: Settings) -> None:
    if not re.match(r"^[A-Za-z0-9_]+$", settings.db_name):
        raise ValueError("DB_NAME must contain only letters, numbers, and _")

    admin_engine = create_engine(
        settings.database_url_for("postgres"),
        pool_pre_ping=True,
        connect_args={"connect_timeout": int(settings.api_timeout), "client_encoding": "UTF8"},
        isolation_level="AUTOCOMMIT",
        future=True,
    )
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": settings.db_name},
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{settings.db_name}"'))
    finally:
        admin_engine.dispose()


def create_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope(session_factory):
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
