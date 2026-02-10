"""
Модуль для парсинга 360-градусных фото (панорам).
"""
from __future__ import annotations

import logging
from typing import Optional

from app.db import session_scope
from app.models import PanoramaColor, PanoramaPhoto, Spec
from app.parser.autohome_client import AutohomeClient
from app.parser.parsers import PanoramaColorData, PanoramaPhotoData, parse_pano_baseinfo, parse_vr_info
from app.repository import upsert_panorama_colors, upsert_panorama_photos

logger = logging.getLogger(__name__)


def find_ext_id_for_spec(
    client: AutohomeClient, 
    spec_id: int, 
    session_factory=None
) -> Optional[int]:
    """
    Пытается найти ext_id для указанного spec_id.
    
    Стратегия:
    1. Проверяет БД на наличие сохраненного ext_id для этого spec_id
    2. Парсит HTML страницу https://pano.autohome.com.cn/car/ext/{spec_id}
    3. Ищет в HTML/JavaScript запрос к /api/ext/baseinfo/{ext_id}
    4. Если не находит, пробует использовать spec_id напрямую как ext_id
    5. Проверяет ext.Id из ответа API, даже если SpecId не совпадает
    
    Args:
        client: Клиент для работы с API
        spec_id: ID комплектации
        session_factory: Фабрика сессий БД (опционально, для проверки сохраненных ext_id)
    
    Returns:
        ext_id если найден, иначе None
    """
    import re
    
    # Вариант 0: Проверяем БД на наличие сохраненного ext_id
    if session_factory:
        try:
            with session_scope(session_factory) as session:
                saved_color = session.query(PanoramaColor).filter(
                    PanoramaColor.spec_id == spec_id,
                    PanoramaColor.ext_id.isnot(None)
                ).first()
                if saved_color and saved_color.ext_id:
                    logger.debug(f"✓ Найден сохраненный ext_id={saved_color.ext_id} для spec_id={spec_id} (из БД)")
                    # Проверяем, что ext_id все еще валиден
                    try:
                        response = client.get_pano_baseinfo(saved_color.ext_id)
                        ext = response.get("ext") or {}
                        if ext.get("SpecId") == spec_id:
                            return saved_color.ext_id
                        else:
                            logger.debug(f"Сохраненный ext_id={saved_color.ext_id} больше не валиден: ext.SpecId={ext.get('SpecId')}, ожидали {spec_id}")
                    except Exception:
                        logger.debug(f"Сохраненный ext_id={saved_color.ext_id} больше не валиден (ошибка API)")
        except Exception as e:
            logger.debug(f"Ошибка при проверке сохраненного ext_id: {e}")
    
    # Вариант 1: Парсим HTML страницу панорамы
    try:
        logger.debug(f"Получение HTML страницы для spec_id={spec_id}...")
        html = client.get_pano_page(spec_id)
        
        # Ищем в HTML запрос к /api/ext/baseinfo/{число}
        # Паттерн: /api/ext/baseinfo/5632 или baseinfo/5632
        patterns = [
            r'/api/ext/baseinfo/(\d+)',  # Полный путь
            r'baseinfo/(\d+)',  # Короткий путь
            r'"extId":\s*(\d+)',  # JSON формат
            r'"ext_id":\s*(\d+)',  # JSON формат с подчеркиванием
            r'extId["\']?\s*[:=]\s*(\d+)',  # JavaScript переменная
            r'ext_id["\']?\s*[:=]\s*(\d+)',  # JavaScript переменная с подчеркиванием
            r'ext\.Id["\']?\s*[:=]\s*(\d+)',  # ext.Id
            r'"Id":\s*(\d+).*?"SpecId":\s*' + str(spec_id),  # Ищем Id рядом с нужным SpecId
            r'pano\.autohome\.com\.cn/car/ext/(\d+)',  # URL страницы
            r'window\.__INITIAL_STATE__\s*=\s*\{[^}]*"extId":\s*(\d+)',  # React/Vue initial state
            r'__INITIAL_STATE__["\']?\s*[:=]\s*\{[^}]*"extId":\s*(\d+)',  # Другой формат
        ]
        
        found_candidates = set()
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
            for match in matches:
                try:
                    candidate_ext_id = int(match)
                    # Исключаем spec_id из кандидатов (он уже будет проверен отдельно)
                    if candidate_ext_id != spec_id:
                        found_candidates.add(candidate_ext_id)
                except (ValueError, TypeError):
                    continue
        
        # Также ищем все числа в URL-ах, которые могут быть ext_id
        # Ищем все URL с /api/ext/baseinfo/
        url_pattern = r'https?://[^"\s]+/api/ext/baseinfo/(\d+)'
        url_matches = re.findall(url_pattern, html, re.IGNORECASE)
        for match in url_matches:
            try:
                candidate_ext_id = int(match)
                if candidate_ext_id != spec_id:
                    found_candidates.add(candidate_ext_id)
            except (ValueError, TypeError):
                continue
        
        logger.debug(f"Найдено кандидатов ext_id: {found_candidates}")
        
        # Если не нашли кандидатов в явных паттернах, пробуем более агрессивный поиск
        if not found_candidates:
            logger.debug("Не найдено кандидатов в явных паттернах, пробуем агрессивный поиск...")
            # Ищем все числа в URL-ах, которые могут быть ext_id
            # Ищем паттерны вида: /5632/ или ?id=5632 или "5632" рядом с "ext" или "pano"
            aggressive_patterns = [
                r'/(\d{4,})/',  # Числа из 4+ цифр в URL
                r'[?&]id=(\d{4,})',  # Параметр id в URL
                r'["\'](\d{4,})["\']',  # Числа в кавычках
                r'ext["\']?\s*[:=]\s*["\']?(\d{4,})',  # ext: 5632 или ext="5632"
                r'pano["\']?\s*[:=]\s*["\']?(\d{4,})',  # pano: 5632
            ]
            
            for pattern in aggressive_patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    try:
                        candidate = int(match)
                        # Исключаем слишком маленькие и слишком большие числа
                        # ext_id обычно в диапазоне 1000-999999
                        if 1000 <= candidate <= 999999 and candidate != spec_id:
                            found_candidates.add(candidate)
                    except (ValueError, TypeError):
                        continue
            
            logger.debug(f"После агрессивного поиска найдено кандидатов: {len(found_candidates)}")
            if found_candidates:
                logger.debug(f"Найдено кандидатов ext_id (агрессивный поиск): {sorted(list(found_candidates))[:10]}")
        
        # Проверяем каждый кандидат
        for candidate_ext_id in sorted(found_candidates):  # Сортируем для предсказуемости
            try:
                response = client.get_pano_baseinfo(candidate_ext_id)
                ext = response.get("ext") or {}
                if ext.get("SpecId") == spec_id:
                    logger.debug(f"✓ Найден ext_id={candidate_ext_id} для spec_id={spec_id} (из HTML страницы)")
                    return candidate_ext_id
                else:
                    logger.debug(f"Кандидат {candidate_ext_id} не подходит: ext.SpecId={ext.get('SpecId')}, ожидали {spec_id}")
            except Exception as e:
                logger.debug(f"Ошибка при проверке кандидата {candidate_ext_id}: {e}")
                continue
                
    except Exception as e:
        logger.debug(f"Не удалось получить HTML страницу для spec_id={spec_id}: {e}")
    
    # Вариант 2: Использовать spec_id напрямую как ext_id
    try:
        logger.debug(f"Пробуем использовать spec_id={spec_id} напрямую как ext_id...")
        response = client.get_pano_baseinfo(spec_id)
        ext = response.get("ext") or {}
        
        # Проверяем, совпадает ли SpecId
        if ext.get("SpecId") == spec_id:
            logger.debug(f"✓ Найден ext_id={spec_id} для spec_id={spec_id} (spec_id == ext_id)")
            return spec_id
        
        # Если SpecId не совпадает, но в ответе есть ext.Id, пробуем его
        ext_id_from_response = ext.get("Id")
        if ext_id_from_response:
            if ext_id_from_response == spec_id:
                # ext.Id совпадает с spec_id, но SpecId не совпадает - это странно, но пробуем
                logger.debug(f"ext.Id={ext_id_from_response} совпадает с spec_id, но ext.SpecId={ext.get('SpecId')} не совпадает")
            else:
                logger.debug(f"В ответе найден ext.Id={ext_id_from_response} (ext.SpecId={ext.get('SpecId')}), пробуем его...")
                try:
                    response2 = client.get_pano_baseinfo(ext_id_from_response)
                    ext2 = response2.get("ext") or {}
                    if ext2.get("SpecId") == spec_id:
                        logger.debug(f"✓ Найден ext_id={ext_id_from_response} для spec_id={spec_id} (из ответа API)")
                        return ext_id_from_response
                    else:
                        logger.debug(f"ext.Id={ext_id_from_response} не подходит: ext.SpecId={ext2.get('SpecId')}, ожидали {spec_id}")
                except Exception as e:
                    logger.debug(f"Ошибка при проверке ext.Id={ext_id_from_response}: {e}")
        
        logger.debug(f"spec_id={spec_id} не подходит как ext_id: ext.SpecId={ext.get('SpecId')}, ext.Id={ext_id_from_response}")
    except Exception as e:
        # Если API вернул ошибку (404, 500 и т.д.), это нормально - значит spec_id не является ext_id
        logger.debug(f"Не удалось использовать spec_id={spec_id} как ext_id: {e}")
    
    logger.warning(f"Не удалось найти ext_id для spec_id={spec_id}")
    return None


def parse_panorama_colors(spec_id: int, session_factory, client: AutohomeClient, ext_id: Optional[int] = None) -> dict:
    """
    Парсит цвета для 360-градусных фото для указанной комплектации.
    
    Args:
        spec_id: ID комплектации
        session_factory: Фабрика сессий БД
        client: Клиент для работы с API
        ext_id: ID панорамы (если None, попытается найти автоматически)
    
    Returns:
        Словарь со статистикой: {"inserted": ..., "updated": ..., "skipped": ...}
    """
    try:
        # Проверяем, существует ли комплектация в БД
        with session_scope(session_factory) as check_session:
            spec_exists = check_session.query(Spec).filter(Spec.id == spec_id).first()
            if not spec_exists:
                error_msg = (
                    f"Комплектация с ID {spec_id} не найдена в базе данных. "
                    f"Сначала запустите основной парсер (app/main.py)."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Пытаемся найти ext_id, если не указан
        if ext_id is None:
            ext_id = find_ext_id_for_spec(client, spec_id, session_factory)
            if ext_id is None:
                logger.warning(f"Не удалось найти ext_id для spec_id={spec_id}, пропускаем")
                return {
                    "colors": {"inserted": 0, "updated": 0, "skipped": 0},
                    "photos": {"inserted": 0, "updated": 0, "skipped": 0},
                }
        
        # Получаем информацию о панораме
        logger.debug(f"Получение информации о панораме для spec_id={spec_id}, ext_id={ext_id}...")
        response = client.get_pano_baseinfo(ext_id)
        
        # Парсим данные
        parsed_data = parse_pano_baseinfo(response, spec_id)
        colors = parsed_data.get("colors", [])
        photos = parsed_data.get("photos", [])
        
        logger.debug(f"Найдено цветов для 360 фото: {len(colors)}, фото: {len(photos)}")
        
        # Логируем информацию о найденных цветах
        if colors:
            logger.debug("Доступные цвета для 360 фото:")
            for color in colors:
                logger.debug(
                    f"  - ColorId: {color.color_id}, "
                    f"Название: {color.color_name}, "
                    f"HEX: {color.color_value or 'N/A'}, "
                    f"ID записи: {color.id}"
                )
        else:
            logger.warning("Не найдено цветов в color_info. Возможно, у этой комплектации нет фото 360.")
        
        # Сохраняем в БД
        with session_scope(session_factory) as session:
            colors_stats = upsert_panorama_colors(session, colors)
            
            # Сохраняем фото, если они есть в ответе
            photos_stats = {"inserted": 0, "updated": 0, "skipped": 0}
            if photos:
                photos_stats = upsert_panorama_photos(session, photos)
            
            # Обновляем ext_id для всех цветов этого spec_id (если он был найден)
            if ext_id and colors:
                updated_count = session.query(PanoramaColor).filter(
                    PanoramaColor.spec_id == spec_id,
                    PanoramaColor.ext_id.is_(None)
                ).update({PanoramaColor.ext_id: ext_id}, synchronize_session=False)
                if updated_count > 0:
                    logger.debug(f"Обновлено ext_id={ext_id} для {updated_count} цветов spec_id={spec_id}")
        
        logger.debug(
            f"Spec {spec_id}: цвета 360 фото - добавлено: {colors_stats['inserted']}, "
            f"обновлено: {colors_stats['updated']}, пропущено: {colors_stats['skipped']}; "
            f"фото - добавлено: {photos_stats['inserted']}, обновлено: {photos_stats['updated']}, "
            f"пропущено: {photos_stats['skipped']}"
        )
        
        return {
            "colors": colors_stats,
            "photos": photos_stats,
        }
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге цветов 360 фото для spec_id {spec_id}: {e}", exc_info=True)
        raise


def parse_panorama_photos(
    spec_id: int,
    session_factory,
    client: AutohomeClient,
) -> dict:
    """
    Парсит 360-градусные фото для указанной комплектации.
    
    Args:
        spec_id: ID комплектации
        session_factory: Фабрика сессий БД
        client: Клиент для работы с API
    
    Returns:
        Словарь со статистикой парсинга
    """
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    with session_scope(session_factory) as session:
        # Проверяем, существует ли комплектация в БД
        spec_exists = session.query(Spec).filter(Spec.id == spec_id).first()
        if not spec_exists:
            error_msg = (
                f"Комплектация с ID {spec_id} не найдена в базе данных. "
                f"Сначала запустите основной парсер (app/main.py)."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Получаем все цвета для 360 фото
        colors = session.query(PanoramaColor).filter(PanoramaColor.spec_id == spec_id).all()
        if not colors:
            logger.warning(f"Не найдено цветов для 360 фото для spec_id {spec_id}. Сначала запустите parse_panorama_colors()")
            return {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        
        logger.debug(
            f"Начало парсинга 360 фото для spec_id {spec_id}: "
            f"{len(colors)} цветов"
        )
        
        # Проходим по всем цветам
        for color in colors:
            try:
                # Получаем список фото 360
                response = client.get_vr_info(
                    spec_id=spec_id,
                    color_id=color.color_id,
                )
                
                # Парсим фото
                photos = parse_vr_info(
                    response,
                    spec_id=spec_id,
                    color_id=color.color_id,
                )
                
                photos_count = len(photos)
                logger.debug(
                    f"Spec {spec_id}, Color {color.color_id} ({color.color_name}): "
                    f"спарсено {photos_count} фото 360"
                )
                
                # Сохраняем фото
                if photos:
                    with session_scope(session_factory) as photo_session:
                        stats = upsert_panorama_photos(photo_session, photos)
                        total_inserted += stats["inserted"]
                        total_updated += stats["updated"]
                        total_skipped += stats["skipped"]
            
            except Exception as e:
                logger.error(
                    f"Ошибка при парсинге 360 фото (Spec {spec_id}, Color {color.color_id}): {e}"
                )
                total_errors += 1
    
    logger.debug(
        f"Парсинг 360 фото для spec_id {spec_id} завершен: "
        f"добавлено: {total_inserted}, обновлено: {total_updated}, "
        f"пропущено: {total_skipped}, ошибок: {total_errors}"
    )
    
    return {
        "inserted": total_inserted,
        "updated": total_updated,
        "skipped": total_skipped,
        "errors": total_errors,
    }
