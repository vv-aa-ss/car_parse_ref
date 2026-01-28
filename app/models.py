from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Brand(Base):
    __tablename__ = "brands"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    logo_url = Column(Text, nullable=True)
    series_count = Column(Integer, nullable=True)
    first_letter = Column(String(8), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    factories = relationship("Factory", back_populates="brand")
    series = relationship("Series", back_populates="brand")


class Factory(Base):
    __tablename__ = "factories"

    id = Column(Integer, primary_key=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False)
    name = Column(String(255), nullable=False)
    real_brand_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    brand = relationship("Brand", back_populates="factories")
    series = relationship("Series", back_populates="factory")


class Series(Base):
    __tablename__ = "series"

    id = Column(Integer, primary_key=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False)
    factory_id = Column(Integer, ForeignKey("factories.id"), nullable=True)
    name = Column(String(255), nullable=False)
    state = Column(Integer, nullable=True)
    is_new_energy = Column(Boolean, nullable=True)
    spec_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    brand = relationship("Brand", back_populates="series")
    factory = relationship("Factory", back_populates="series")
    specs = relationship("Spec", back_populates="series")


class Spec(Base):
    __tablename__ = "specs"

    id = Column(Integer, primary_key=True)
    series_id = Column(Integer, ForeignKey("series.id"), nullable=False)
    name = Column(String(255), nullable=False)
    spec_status = Column(Integer, nullable=True)
    year = Column(String(32), nullable=True)
    min_price = Column(String(64), nullable=True)
    dealer_price = Column(String(64), nullable=True)
    condition = Column(JSON, nullable=True)
    sort = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", back_populates="specs")
    param_values = relationship("ParamValue", back_populates="spec")


class ParamTitle(Base):
    __tablename__ = "param_titles"

    item_id = Column(Integer, primary_key=True)
    title_id = Column(Integer, nullable=False)
    item_name = Column(String(512), nullable=False)  # Увеличено с 255 до 512
    group_name = Column(String(255), nullable=True)
    item_type = Column(String(64), nullable=True)
    sort = Column(Integer, nullable=True)
    baike_url = Column(Text, nullable=True)
    baike_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ParamValue(Base):
    __tablename__ = "param_values"

    spec_id = Column(Integer, ForeignKey("specs.id"), primary_key=True)
    title_id = Column(Integer, primary_key=True)
    item_name = Column(String(512), primary_key=True)  # Увеличено с 255 до 512
    sub_name = Column(String(512), primary_key=True, nullable=True)  # Увеличено с 255 до 512
    value = Column(Text, nullable=True)
    price_info = Column(String(255), nullable=True)
    video_url = Column(Text, nullable=True)
    color_info = Column(JSON, nullable=True)
    raw = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    spec = relationship("Spec", back_populates="param_values")
