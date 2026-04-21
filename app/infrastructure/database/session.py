from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.infrastructure.database.config import get_settings


def create_engine_from_settings(database_url: str | None = None) -> Engine:
    url = database_url or get_settings().database_url
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, connect_args=connect_args)


def create_session_factory(
    database_url: str | None = None,
    engine: Engine | None = None,
) -> sessionmaker[Session]:
    engine = engine or create_engine_from_settings(database_url)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
