from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
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
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", back_populates="brand")


class Series(Base):
    __tablename__ = "series"

    id = Column(Integer, primary_key=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False)
    name = Column(String(255), nullable=False)
    is_new_energy = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    brand = relationship("Brand", back_populates="series")
    specs = relationship("Spec", back_populates="series")


class Spec(Base):
    __tablename__ = "specification"

    id = Column(Integer, primary_key=True)
    series_id = Column(Integer, ForeignKey("series.id"), nullable=False)
    name = Column(String(255), nullable=False)
    min_price = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", back_populates="specs")
    param_values = relationship("ParamValue", back_populates="spec")


class ParamTitle(Base):
    __tablename__ = "param_titles"

    series_id = Column(Integer, ForeignKey("series.id"), primary_key=True)
    title_id = Column(Integer, primary_key=True)
    item_name = Column(String(512), nullable=False)
    group_name = Column(String(255), nullable=True)
    item_type = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", backref="param_titles")


class ParamValue(Base):
    __tablename__ = "param_values"

    specification_id = Column(Integer, ForeignKey("specification.id"), primary_key=True)
    title_id = Column(Integer, primary_key=True)
    item_name = Column(String(512), primary_key=True)  # Увеличено с 255 до 512
    sub_name = Column(String(512), primary_key=True, nullable=True)  # Увеличено с 255 до 512
    value = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    spec = relationship("Spec", back_populates="param_values", foreign_keys=[specification_id])


class PhotoColor(Base):
    __tablename__ = "photo_colors"

    id = Column(Integer, primary_key=True)
    series_id = Column(Integer, ForeignKey("series.id"), nullable=False)
    color_type = Column(String(16), nullable=False)  # "interior" или "exterior"
    name = Column(String(255), nullable=False)
    value = Column(String(255), nullable=True)  # HEX код цвета

    isonsale = Column(Boolean, nullable=True)  # В продаже
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", backref="photo_colors")


class PhotoCategory(Base):
    __tablename__ = "photo_categories"

    id = Column(Integer, primary_key=True)
    series_id = Column(Integer, ForeignKey("series.id"), primary_key=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", backref="photo_categories")


class Photo(Base):
    __tablename__ = "photos"

    id = Column(String(64), primary_key=True)  # ID фото из API (может быть строкой)
    series_id = Column(Integer, ForeignKey("series.id"), nullable=False)
    specification_id = Column(Integer, ForeignKey("specification.id"), nullable=False)
    category_id = Column(Integer, nullable=False)  # ID категории из photo_categories
    color_id = Column(Integer, nullable=False)  # ID цвета из photo_colors (0 если не указан)
    originalpic = Column(Text, nullable=True)  # Оригинальная ссылка

    specname = Column(String(255), nullable=True)  # Название комплектации
    local_path = Column(Text, nullable=True)  # Локальный путь к оригинальному фото
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    series = relationship("Series", backref="photos")
    spec = relationship("Spec", backref="photos", foreign_keys=[specification_id])


class PanoramaColor(Base):
    """Цвета для 360-градусных фото (панорам)."""
    __tablename__ = "panorama_colors"

    id = Column(Integer, primary_key=True)  # ID из color_info (Id)
    spec_id = Column(Integer, ForeignKey("specification.id"), nullable=False)
    ext_id = Column(Integer, nullable=True)  # ID панорамы (ext.Id)
    base_color_name = Column(String(255), nullable=True)  # BaseColorName
    color_name = Column(String(255), nullable=False)  # ColorName
    color_value = Column(String(16), nullable=True)  # ColorValue (HEX)
    color_id = Column(Integer, nullable=False)  # ColorId (используется в getVrInfo)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    spec = relationship("Spec", backref="panorama_colors")


class PanoramaPhoto(Base):
    """360-градусные фото (панорамы)."""
    __tablename__ = "panorama_photos"

    id = Column(String(64), primary_key=True)  # ID фото (seq + spec_id + color_id)
    spec_id = Column(Integer, ForeignKey("specification.id"), nullable=False)
    color_id = Column(Integer, nullable=False)  # ColorId из PanoramaColor
    seq = Column(Integer, nullable=False)  # Порядковый номер кадра
    url = Column(Text, nullable=False)  # URL фото
    local_path = Column(Text, nullable=True)  # Локальный путь к фото
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    spec = relationship("Spec", backref="panorama_photos")
