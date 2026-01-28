from typing import Dict, Iterable

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models import Brand, Factory, ParamTitle, ParamValue, Series, Spec
from app.parser.parsers import (
    BrandData,
    FactoryData,
    ParamTitleData,
    ParamValueData,
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
            existing.series_count = item.series_count
            existing.first_letter = item.first_letter
            updated += 1
        else:
            session.add(
                Brand(
                    id=item.id,
                    name=item.name,
                    logo_url=item.logo_url,
                    series_count=item.series_count,
                    first_letter=item.first_letter,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_factories(session: Session, items: Iterable[FactoryData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    for item in items:
        if item.id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.id)
        
        existing = session.query(Factory).filter(Factory.id == item.id).first()
        if existing:
            existing.brand_id = item.brand_id
            existing.name = item.name
            existing.real_brand_id = item.real_brand_id
            updated += 1
        else:
            session.add(
                Factory(
                    id=item.id,
                    brand_id=item.brand_id,
                    name=item.name,
                    real_brand_id=item.real_brand_id,
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
            existing.factory_id = item.factory_id
            existing.name = item.name
            existing.state = item.state
            existing.is_new_energy = item.is_new_energy
            existing.spec_count = item.spec_count
            updated += 1
        else:
            session.add(
                Series(
                    id=item.id,
                    brand_id=item.brand_id,
                    factory_id=item.factory_id,
                    name=item.name,
                    state=item.state,
                    is_new_energy=item.is_new_energy,
                    spec_count=item.spec_count,
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
            existing.spec_status = item.spec_status
            existing.year = item.year
            existing.min_price = item.min_price
            existing.dealer_price = item.dealer_price
            existing.condition = item.condition
            existing.sort = item.sort
            updated += 1
        else:
            session.add(
                Spec(
                    id=item.id,
                    series_id=item.series_id,
                    name=item.name,
                    spec_status=item.spec_status,
                    year=item.year,
                    min_price=item.min_price,
                    dealer_price=item.dealer_price,
                    condition=item.condition,
                    sort=item.sort,
                )
            )
            inserted += 1
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def upsert_param_titles(session: Session, items: Iterable[ParamTitleData]) -> Dict[str, int]:
    seen_ids = set()
    inserted = 0
    updated = 0
    skipped = 0
    
    # Собираем все items сначала, чтобы избежать дубликатов в одном батче
    items_list = list(items)
    unique_items = {}
    for item in items_list:
        if item.item_id not in unique_items:
            unique_items[item.item_id] = item
    
    if not unique_items:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    
    # Используем PostgreSQL ON CONFLICT для потокобезопасного upsert
    values_to_insert = []
    for item in unique_items.values():
        if item.item_id in seen_ids:
            skipped += 1
            continue
        seen_ids.add(item.item_id)
        
        # Обрезаем длинные строки
        item_name = (item.item_name[:512] if item.item_name else "")[:512]
        
        values_to_insert.append({
            "item_id": item.item_id,
            "title_id": item.title_id,
            "item_name": item_name,
            "group_name": item.group_name,
            "item_type": item.item_type,
            "sort": item.sort,
            "baike_url": item.baike_url,
            "baike_id": item.baike_id,
        })
    
    if not values_to_insert:
        return {"inserted": 0, "updated": 0, "skipped": skipped}
    
    # Используем PostgreSQL ON CONFLICT DO UPDATE для потокобезопасного upsert
    stmt = pg_insert(ParamTitle).values(values_to_insert)
    stmt = stmt.on_conflict_do_update(
        index_elements=["item_id"],
        set_={
            "title_id": stmt.excluded.title_id,
            "item_name": stmt.excluded.item_name,
            "group_name": stmt.excluded.group_name,
            "item_type": stmt.excluded.item_type,
            "sort": stmt.excluded.sort,
            "baike_url": stmt.excluded.baike_url,
            "baike_id": stmt.excluded.baike_id,
            "updated_at": stmt.excluded.updated_at,
        }
    )
    
    session.execute(stmt)
    session.flush()
    
    # Подсчитываем: все записи либо вставлены, либо обновлены
    # PostgreSQL не возвращает точное количество, поэтому считаем все как inserted
    inserted = len(values_to_insert)
    updated = 0  # Не можем точно определить без дополнительных запросов
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


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
        key = (item.spec_id, item.title_id, item_name, sub_name)
        if key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(key)
        items_to_process.append((key, item, item_name, sub_name))
    
    if not items_to_process:
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    
    # Загружаем существующие записи одним запросом
    # Ищем по spec_id и title_id, так как item_name может быть неправильным в старых данных
    spec_ids = list(set(k[0] for k, _, _, _ in items_to_process))
    title_ids = list(set(k[1] for k, _, _, _ in items_to_process))
    
    # Группируем по (spec_id, title_id, sub_name) для поиска существующих записей
    # item_name может быть неправильным в старых данных, поэтому ищем по title_id
    existing_values_by_key = {}
    existing_values_by_title = {}  # Для поиска по (spec_id, title_id, sub_name)
    
    for v in (
        session.query(ParamValue)
        .filter(ParamValue.spec_id.in_(spec_ids), ParamValue.title_id.in_(title_ids))
        .all()
    ):
        key = (v.spec_id, v.title_id, v.item_name, v.sub_name or "")
        existing_values_by_key[key] = v
        
        # Также создаем индекс по (spec_id, title_id, sub_name) для поиска с неправильным item_name
        title_key = (v.spec_id, v.title_id, v.sub_name or "")
        if title_key not in existing_values_by_title:
            existing_values_by_title[title_key] = []
        existing_values_by_title[title_key].append(v)
    
    for key, item, item_name, sub_name in items_to_process:
        # Ищем все существующие записи по (spec_id, title_id, sub_name)
        # item_name может быть неправильным в старых данных
        title_key = (item.spec_id, item.title_id, sub_name)
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
            existing_correct.price_info = item.price_info
            existing_correct.video_url = item.video_url
            existing_correct.color_info = item.color_info
            existing_correct.raw = item.raw
            updated += 1
        else:
            # Создаем новую запись
            session.add(
                ParamValue(
                    spec_id=item.spec_id,
                    title_id=item.title_id,
                    item_name=item_name,
                    sub_name=sub_name,
                    value=item.value,
                    price_info=item.price_info,
                    video_url=item.video_url,
                    color_info=item.color_info,
                    raw=item.raw,
                )
            )
            inserted += 1
    
    if inserted > 0 or updated > 0:
        session.flush()
    
    return {"inserted": inserted, "updated": updated, "skipped": skipped}
