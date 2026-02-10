"""
Модуль для парсинга информации о фотографиях автомобилей.
"""
from __future__ import annotations

import logging
import time

from app.db import session_scope
from app.models import PhotoCategory, PhotoColor, Series, Spec
from app.parser.autohome_client import AutohomeClient
from app.parser.parsers import parse_photo_info, parse_pic_list
from app.repository import upsert_photo_categories, upsert_photo_colors, upsert_photos


logger = logging.getLogger(__name__)


def parse_foto(series_id: int, session_factory, client: AutohomeClient) -> dict:
    """
    Парсит информацию о фотографиях для указанной серии автомобиля.
    
    Args:
        series_id: ID серии автомобиля
        session_factory: Фабрика сессий БД
        client: Клиент для работы с API Autohome
    
    Returns:
        Словарь со статистикой: {"colors": {"inserted": ..., "updated": ..., "skipped": ...},
                                  "categories": {"inserted": ..., "updated": ..., "skipped": ...}}
    """
    try:
        # Проверяем, существует ли серия в БД
        with session_scope(session_factory) as check_session:
            series_exists = check_session.query(Series).filter(Series.id == series_id).first()
            if not series_exists:
                error_msg = (
                    f"Серия с ID {series_id} не найдена в базе данных. "
                    f"Сначала запустите основной парсер (app/main.py), чтобы создать серии в БД."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Получаем информацию о фото
        logger.debug(f"Получение информации о фото для серии {series_id}...")
        response = client.get_series_base_pic_info(series_id)
        
        # Парсим данные
        parsed_data = parse_photo_info(response, series_id)
        colors = parsed_data.get("colors", [])
        categories = parsed_data.get("categories", [])
        
        logger.debug(
            f"Найдено цветов: {len(colors)} (interior + exterior), "
            f"категорий: {len(categories)}"
        )
        
        # Сохраняем в БД
        with session_scope(session_factory) as session:
            colors_stats = upsert_photo_colors(session, colors)
            categories_stats = upsert_photo_categories(session, categories)
        
        logger.debug(
            f"Серия {series_id}: цвета - добавлено: {colors_stats['inserted']}, "
            f"обновлено: {colors_stats['updated']}, пропущено: {colors_stats['skipped']}; "
            f"категории - добавлено: {categories_stats['inserted']}, "
            f"обновлено: {categories_stats['updated']}, пропущено: {categories_stats['skipped']}"
        )
        
        return {
            "colors": colors_stats,
            "categories": categories_stats,
        }
    except Exception as e:
        logger.error(f"Ошибка при парсинге фото для серии {series_id}: {e}", exc_info=True)
        raise


def parse_all_photos(
    series_id: int,
    session_factory,
    client: AutohomeClient,
    page_size: int,
    max_combinations: int = 0,
    skip_spec_ids: set[int] | None = None,
    only_category_ids: list[int] | None = None,
    max_colors: int = 0,
) -> dict:
    """
    Парсит все фотографии для указанной серии автомобиля.
    Проходит по комбинациям: spec × color × category (первая страница — PAGESIZE фото).
    
    Args:
        series_id: ID серии автомобиля
        session_factory: Фабрика сессий БД
        client: Клиент для работы с API Autohome
        page_size: Размер страницы (из конфига PAGESIZE)
        max_combinations: Максимальное количество комбинаций (0 = без ограничений)
        skip_spec_ids: Множество spec_id, которые нужно пропустить (имеют 360 фото)
        only_category_ids: Список ID категорий, которые нужно парсить
        max_colors: Макс. кол-во цветов на тип (exterior/interior). 0 = без лимита.
    
    Returns:
        Словарь со статистикой парсинга
    """
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    with session_scope(session_factory) as session:
        series_exists = session.query(Series).filter(Series.id == series_id).first()
        if not series_exists:
            error_msg = (
                f"Серия с ID {series_id} не найдена в базе данных. "
                f"Сначала запустите основной парсер (app/main.py), чтобы создать серии в БД."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        specs = session.query(Spec).filter(Spec.series_id == series_id).all()
        all_colors = session.query(PhotoColor).filter(PhotoColor.series_id == series_id).all()
        categories = session.query(PhotoCategory).filter(PhotoCategory.series_id == series_id).all()
        
        if not specs:
            logger.warning(f"Не найдено комплектаций для серии {series_id}")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        if not all_colors:
            logger.warning(f"Не найдено цветов для серии {series_id}. Сначала запустите parse_foto()")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        if not categories:
            logger.warning(f"Не найдено категорий для серии {series_id}. Сначала запустите parse_foto()")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        # Разделяем цвета на exterior/interior
        exterior_colors = [c for c in all_colors if c.color_type == "exterior"]
        interior_colors = [c for c in all_colors if c.color_type == "interior"]
        
        # Лимит цветов на тип (MAX_COLORS)
        if max_colors > 0:
            orig_ext = len(exterior_colors)
            orig_int = len(interior_colors)
            exterior_colors = exterior_colors[:max_colors]
            interior_colors = interior_colors[:max_colors]
            if orig_ext > max_colors or orig_int > max_colors:
                logger.info(
                    f"MAX_COLORS={max_colors}: exterior {orig_ext}→{len(exterior_colors)}, "
                    f"interior {orig_int}→{len(interior_colors)}"
                )
        
        # Фильтрация по режиму 360only
        if skip_spec_ids:
            original_specs_count = len(specs)
            specs = [s for s in specs if s.id not in skip_spec_ids]
            skipped_360 = original_specs_count - len(specs)
            if skipped_360 > 0:
                logger.info(
                    f"360only: пропущено {skipped_360} комплектаций с 360 фото "
                    f"(осталось {len(specs)} для парсинга обычных фото)"
                )
        
        if only_category_ids is not None:
            original_categories_count = len(categories)
            only_category_ids_set = set(only_category_ids)
            categories = [c for c in categories if c.id in only_category_ids_set]
            skipped_cats = original_categories_count - len(categories)
            if skipped_cats > 0:
                logger.info(
                    f"360only: отфильтровано категорий: оставлено {len(categories)} из {original_categories_count} "
                    f"(ID категорий: {only_category_ids})"
                )
        
        if not specs:
            logger.info(f"360only: все комплектации серии {series_id} имеют 360 фото, пропускаем обычные фото")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        if not categories:
            logger.warning(f"360only: после фильтрации не осталось категорий для серии {series_id}")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        # Собираем список (color, is_inner) для итерации
        color_pairs = [(c, False) for c in exterior_colors] + [(c, True) for c in interior_colors]
        
        specs_count = len(specs)
        colors_count = len(color_pairs)
        categories_count = len(categories)
        total_combinations = specs_count * colors_count * categories_count
        
        logger.debug(f"Парсинг фото для серии {series_id}: "
                      f"{specs_count} specs × {colors_count} цветов "
                      f"({len(exterior_colors)} ext + {len(interior_colors)} int) × "
                      f"{categories_count} cat = {total_combinations} API-вызовов")
        
        # Применяем ограничение на количество комбинаций
        if max_combinations > 0 and total_combinations > max_combinations:
            logger.info(
                f"  Ограничение: {max_combinations} из {total_combinations} "
                f"(MAX_PHOTO_COMBINATIONS={max_combinations})"
            )
            total_combinations = max_combinations
        
        processed_combinations = 0
        should_stop = False
        
        # Собираем все фото в батч для более эффективного сохранения
        all_photos_batch = []
        batch_size = 200
        
        for spec in specs:
            if should_stop:
                break
            for color, is_inner in color_pairs:
                if should_stop:
                    break
                for category in categories:
                    if max_combinations > 0 and processed_combinations >= max_combinations:
                        should_stop = True
                        break
                    
                    processed_combinations += 1
                    
                    try:
                        response = client.get_pic_list(
                            series_id=series_id,
                            spec_id=spec.id,
                            category_id=category.id,
                            color_id=color.id,
                            is_inner=is_inner,
                            page_size=page_size,
                            page_index=1,
                        )
                        
                        parsed = parse_pic_list(
                            response,
                            series_id=series_id,
                            spec_id=spec.id,
                            category_id=category.id,
                            color_id=color.id,
                        )
                        
                        pagecount = parsed.get("pagecount", 0)
                        rowcount = parsed.get("rowcount", 0)
                        
                        if pagecount == 0:
                            continue
                        
                        photos = parsed.get("photos", [])
                        
                        if photos:
                            logger.debug(
                                f"[{processed_combinations}/{total_combinations}] "
                                f"Spec {spec.id}, Color {color.id}, Cat {category.id}: "
                                f"{len(photos)} фото (всего: {rowcount})"
                            )
                            all_photos_batch.extend(photos)
                            
                            if len(all_photos_batch) >= batch_size:
                                db_start = time.time()
                                with session_scope(session_factory) as photo_session:
                                    stats = upsert_photos(photo_session, all_photos_batch)
                                    total_inserted += stats["inserted"]
                                    total_updated += stats["updated"]
                                    total_skipped += stats["skipped"]
                                db_time = time.time() - db_start
                                if db_time > 1.0:
                                    logger.debug(f"Батч {len(all_photos_batch)} фото: {db_time:.2f} сек")
                                all_photos_batch = []
                    
                    except Exception as e:
                        logger.error(
                            f"Ошибка (Spec {spec.id}, Color {color.id}, Cat {category.id}): {e}"
                        )
                        total_errors += 1
        
        # Сохраняем оставшиеся фото из батча
        if all_photos_batch:
            with session_scope(session_factory) as photo_session:
                stats = upsert_photos(photo_session, all_photos_batch)
                total_inserted += stats["inserted"]
                total_updated += stats["updated"]
                total_skipped += stats["skipped"]
        
        logger.debug(
            f"Серия {series_id} завершена: "
            f"+{total_inserted} обновлено {total_updated} "
            f"пропущено {total_skipped} ошибок {total_errors}"
        )
    
    return {
        "inserted": total_inserted,
        "updated": total_updated,
        "skipped": total_skipped,
        "errors": total_errors,
    }
