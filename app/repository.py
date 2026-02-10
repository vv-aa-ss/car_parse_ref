from typing import Dict, Iterable

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models import (
    Brand,
    ParamTitle,
    ParamValue,
    PanoramaColor,
    PanoramaPhoto,
    Photo,
    PhotoCategory,
    PhotoColor,
    Series,
    Spec,
)
from app.parser.parsers import (
    BrandData,
    ParamTitleData,
    ParamValueData,
    PanoramaColorData,
    PanoramaPhotoData,
    PhotoCategoryData,
    PhotoColorData,
    PhotoData,
    SeriesData,
    SpecData,
)


def upsert_brands(session: Session, items: Iterable[BrandData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(Brand).filter(Brand.id == item.id).first()
        if existing:
            existing.name = item.name
            existing.logo_url = item.logo_url
            updated += 1
        else:
            session.add(
                Brand(
                    id=item.id,
                    name=item.name,
                    logo_url=item.logo_url,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_series(session: Session, items: Iterable[SeriesData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(Series).filter(Series.id == item.id).first()
        if existing:
            existing.brand_id = item.brand_id
            existing.name = item.name
            existing.is_new_energy = item.is_new_energy
            updated += 1
        else:
            session.add(
                Series(
                    id=item.id,
                    brand_id=item.brand_id,
                    name=item.name,
                    is_new_energy=item.is_new_energy,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_specs(session: Session, items: Iterable[SpecData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(Spec).filter(Spec.id == item.id).first()
        if existing:
            existing.series_id = item.series_id
            existing.name = item.name
            existing.min_price = item.min_price
            updated += 1
        else:
            session.add(
                Spec(
                    id=item.id,
                    series_id=item.series_id,
                    name=item.name,
                    min_price=item.min_price,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_param_titles(session: Session, items: Iterable[ParamTitleData]) -> Dict[str, int]:
    inserted = 0
    skipped = 0
    
    # Дедупликация по (series_id, title_id) — составной PK
    items_list = list(items)
    unique_items: dict[tuple[int, int], ParamTitleData] = {}
    for item in items_list:
        key = (item.series_id, item.title_id)
        if key not in unique_items:
            unique_items[key] = item
        else:
            skipped += 1
    
    if not unique_items:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    
    values_to_insert = []
    for item in unique_items.values():
        item_name = (item.item_name[:512] if item.item_name else "")[:512]
        values_to_insert.append({
            "series_id": item.series_id,
            "title_id": item.title_id,
            "item_name": item_name,
            "group_name": item.group_name,
            "item_type": item.item_type,
        })
    
    if not values_to_insert:
        return {"inserted": 0, "updated": 0, "skipped": skipped}
    
    # ON CONFLICT по составному PK (series_id, title_id)
    stmt = pg_insert(ParamTitle).values(values_to_insert)
    stmt = stmt.on_conflict_do_update(
        index_elements=["series_id", "title_id"],
        set_={
            "item_name": stmt.excluded.item_name,
            "group_name": stmt.excluded.group_name,
            "item_type": stmt.excluded.item_type,
            "updated_at": stmt.excluded.updated_at,
        }
    )
    
    session.execute(stmt)
    session.flush()
    
    inserted = len(values_to_insert)
    
    return {"inserted": inserted, "updated": 0, "skipped": skipped}


def upsert_param_values(session: Session, items: Iterable[ParamValueData]) -> Dict[str, int]:
    seen_keys = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    # Собираем все items и обрезаем длинные строки
    items_to_process = []
    for item in items:
        # Используем пустую строку вместо None для sub_name
        sub_name = (item.sub_name or "")[:512]  # Обрезаем до 512 символов
        item_name = (item.item_name or "")[:512]  # Обрезаем до 512 символов
        key = (item.specification_id, item.title_id, item_name, sub_name)
        if key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(key)
        items_to_process.append((key, item, item_name, sub_name))
    
    if not items_to_process:
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    
    # Загружаем существующие записи одним запросом
    # Ищем по specification_id и title_id, так как item_name может быть неправильным в старых данных
    spec_ids = list(set(k[0] for k, _, _, _ in items_to_process))
    title_ids = list(set(k[1] for k, _, _, _ in items_to_process))
    
    # Группируем по (specification_id, title_id, sub_name) для поиска существующих записей
    # item_name может быть неправильным в старых данных, поэтому ищем по title_id
    existing_values_by_key = {}
    existing_values_by_title = {}  # Для поиска по (specification_id, title_id, sub_name)
    
    for v in (
        session.query(ParamValue)
        .filter(ParamValue.specification_id.in_(spec_ids), ParamValue.title_id.in_(title_ids))
        .all()
    ):
        key = (v.specification_id, v.title_id, v.item_name, v.sub_name or "")
        existing_values_by_key[key] = v
        
        # Также создаем индекс по (specification_id, title_id, sub_name) для поиска с неправильным item_name
        title_key = (v.specification_id, v.title_id, v.sub_name or "")
        if title_key not in existing_values_by_title:
            existing_values_by_title[title_key] = []
        existing_values_by_title[title_key].append(v)
    
    for key, item, item_name, sub_name in items_to_process:
        # Ищем все существующие записи по (specification_id, title_id, sub_name)
        # item_name может быть неправильным в старых данных
        title_key = (item.specification_id, item.title_id, sub_name)
        candidates = existing_values_by_title.get(title_key, [])
        
        # Находим запись с правильным item_name (если есть)
        existing_correct = None
        old_records_to_delete = []
        
        for candidate in candidates:
            if candidate.item_name == item_name:
                existing_correct = candidate
            else:
                # Запись с неправильным item_name - нужно удалить
                old_records_to_delete.append(candidate)
        
        # Удаляем все старые записи с неправильным item_name
        for old_record in old_records_to_delete:
            session.delete(old_record)
        
        if old_records_to_delete:
            session.flush()  # Flush после удаления
        
        if existing_correct:
            # Обновляем существующую запись с правильным item_name
            existing_correct.value = item.value
            updated += 1
        else:
            # Создаем новую запись
            session.add(
                ParamValue(
                    specification_id=item.specification_id,
                    title_id=item.title_id,
                    item_name=item_name,
                    sub_name=sub_name,
                    value=item.value,
                )
            )
            inserted += 1
    
    if inserted > 0 or updated > 0:
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_photo_colors(session: Session, items: Iterable[PhotoColorData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(PhotoColor).filter(PhotoColor.id == item.id).first()
        if existing:
            existing.series_id = item.series_id
            existing.color_type = item.color_type
            existing.name = item.name
            existing.value = item.value

            existing.isonsale = item.isonsale
            updated += 1
        else:
            session.add(
                PhotoColor(
                    id=item.id,
                    series_id=item.series_id,
                    color_type=item.color_type,
                    name=item.name,
                    value=item.value,

                    isonsale=item.isonsale,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_photo_categories(session: Session, items: Iterable[PhotoCategoryData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        # Для категорий используем составной ключ (series_id, id), так как id категории может повторяться для разных серий
        key = (item.series_id, item.id)
        if key in seen_ids:
            skipped += 1
            continue
        seen_ids.add(key)
        
        existing = session.query(PhotoCategory).filter(
            PhotoCategory.series_id == item.series_id,
            PhotoCategory.id == item.id
        ).first()
        if existing:
            existing.name = item.name
            updated += 1
        else:
            session.add(
                PhotoCategory(
                    id=item.id,
                    series_id=item.series_id,
                    name=item.name,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_photos(session: Session, items: Iterable[PhotoData]) -> Dict[str, int]:
    """
    Оптимизированный upsert для фото: использует PostgreSQL ON CONFLICT DO UPDATE для максимальной производительности.
    """
    items_list = list(items)
    if not items_list:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    
    seen_ids = set()
    skipped = 0
    
    # Собираем уникальные items
    unique_items = {}
    for item in items_list:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        unique_items[item.id] = item
    
    if not unique_items:
        return {"inserted": 0, "updated": 0, "skipped": skipped}
    
    # Используем PostgreSQL ON CONFLICT для потокобезопасного upsert
    values_to_insert = []
    for item in unique_items.values():
        values_to_insert.append({
            "id": item.id,
            "series_id": item.series_id,
            "specification_id": item.specification_id,
            "category_id": item.category_id,
            "color_id": item.color_id,
            "originalpic": item.originalpic,

            "specname": item.specname,
        })
    
    if not values_to_insert:
        return {"inserted": 0, "updated": 0, "skipped": skipped}
    
    # Используем ON CONFLICT DO UPDATE для быстрого upsert
    stmt = pg_insert(Photo).values(values_to_insert)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "series_id": stmt.excluded.series_id,
            "specification_id": stmt.excluded.specification_id,
            "category_id": stmt.excluded.category_id,
            "color_id": stmt.excluded.color_id,
            "originalpic": stmt.excluded.originalpic,

            "specname": stmt.excluded.specname,
            "updated_at": stmt.excluded.updated_at,
        }
    )
    
    session.execute(stmt)
    session.flush()
    
    # PostgreSQL не возвращает точное количество inserted/updated, считаем все как inserted
    inserted = len(values_to_insert)
    updated = 0  # Не можем точно определить без дополнительных запросов
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_panorama_colors(session: Session, items: Iterable[PanoramaColorData]) -> Dict[str, int]:
    """Upsert для цветов 360-градусных фото."""
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(PanoramaColor).filter(PanoramaColor.id == item.id).first()
        if existing:
            existing.spec_id = item.spec_id
            existing.ext_id = item.ext_id
            existing.base_color_name = item.base_color_name
            existing.color_name = item.color_name
            existing.color_value = item.color_value
            existing.color_id = item.color_id
            updated += 1
        else:
            session.add(
                PanoramaColor(
                    id=item.id,
                    spec_id=item.spec_id,
                    ext_id=item.ext_id,
                    base_color_name=item.base_color_name,
                    color_name=item.color_name,
                    color_value=item.color_value,
                    color_id=item.color_id,
                )
            )
            inserted += 1
    
    if inserted > 0 or updated > 0:
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_panorama_photos(session: Session, items: Iterable[PanoramaPhotoData]) -> Dict[str, int]:
    """Upsert для 360-градусных фото."""
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(PanoramaPhoto).filter(PanoramaPhoto.id == item.id).first()
        if existing:
            existing.spec_id = item.spec_id
            existing.color_id = item.color_id
            existing.seq = item.seq
            existing.url = item.url
            updated += 1
        else:
            session.add(
                PanoramaPhoto(
                    id=item.id,
                    spec_id=item.spec_id,
                    color_id=item.color_id,
                    seq=item.seq,
                    url=item.url,
                )
            )
            inserted += 1
    
    if inserted > 0 or updated > 0:
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}
