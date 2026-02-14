"""
Модуль для загрузки фотографий в файловую систему.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Сигнатуры (magic bytes) для валидации изображений
_IMAGE_SIGNATURES = {
    b'\xff\xd8\xff': 'JPEG',
    b'\x89PNG': 'PNG',
    b'GIF8': 'GIF',
    # WebP: RIFF....WEBP
    b'RIFF': 'RIFF/WebP',
}

# Минимальный размер изображения (в байтах). Файлы меньше — скорее всего не изображения.
_MIN_IMAGE_SIZE = 1024  # 1 КБ


def _ensure_https(url: str) -> str:
    """Конвертирует http:// в https:// для защиты от перехвата ISP."""
    if url and url.startswith("http://"):
        return "https://" + url[7:]
    return url


def _is_valid_image_content(data: bytes) -> bool:
    """
    Проверяет, что данные начинаются с сигнатуры известного формата изображений.
    
    Returns:
        True если данные похожи на изображение, False иначе
    """
    if len(data) < 4:
        return False
    
    for signature in _IMAGE_SIGNATURES:
        if data[:len(signature)] == signature:
            # Дополнительная проверка для WebP: RIFF....WEBP
            if signature == b'RIFF' and len(data) >= 12:
                if data[8:12] != b'WEBP':
                    continue  # RIFF, но не WebP — не наш формат
            return True
    
    return False


def get_file_extension(url: str) -> str:
    """Определяет расширение файла из URL."""
    parsed = urlparse(url)
    path = parsed.path
    # Убираем параметры после точки (например, .webp)
    if '.' in path:
        ext = path.rsplit('.', 1)[1].lower()
        # Ограничиваем длину расширения
        if len(ext) > 5:
            ext = ext[:5]
        return f".{ext}"
    return ".jpg"  # По умолчанию jpg


def download_image(url: str, file_path: Path, timeout: float = 10.0, max_retries: int = 3) -> bool:
    """
    Скачивает изображение по URL и сохраняет в файл.
    
    Включает защиту от подмены контента провайдером (ISP):
    - Конвертирует http:// в https://
    - Проверяет Content-Type заголовок
    - Валидирует magic bytes изображения
    - Проверяет минимальный размер файла
    
    Args:
        url: URL изображения
        file_path: Путь для сохранения файла
        timeout: Таймаут запроса
        max_retries: Максимальное количество повторных попыток
    
    Returns:
        True если успешно, False в противном случае
    """
    if file_path.exists():
        # Проверяем, не является ли существующий файл "битым" (слишком маленький / не изображение)
        file_size = file_path.stat().st_size
        if file_size < _MIN_IMAGE_SIZE:
            logger.warning(
                f"Существующий файл подозрительно мал ({file_size} Б), "
                f"перекачиваем: {file_path}"
            )
            file_path.unlink()
        else:
            # Быстрая проверка magic bytes у существующего файла
            with open(file_path, 'rb') as f:
                header = f.read(12)
            if not _is_valid_image_content(header):
                logger.warning(
                    f"Существующий файл не является изображением, "
                    f"перекачиваем: {file_path}"
                )
                file_path.unlink()
            else:
                logger.debug(f"Файл уже существует: {file_path}")
                return True
    
    # Конвертируем HTTP в HTTPS для защиты от перехвата ISP
    safe_url = _ensure_https(url)
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    
    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.get(safe_url, headers=headers, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Проверяем Content-Type заголовок
            content_type = response.headers.get('Content-Type', '').lower()
            if content_type and not content_type.startswith('image/'):
                # Если Content-Type явно не image/* — это подмена (HTML, text и т.д.)
                logger.warning(
                    f"Ответ не является изображением (Content-Type: {content_type}), "
                    f"URL: {safe_url}"
                )
                response.close()
                return False
            
            # Создаем директорию если нужно
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Скачиваем содержимое
            content = response.content
            
            # Проверяем минимальный размер
            if len(content) < _MIN_IMAGE_SIZE:
                logger.warning(
                    f"Размер ответа слишком мал ({len(content)} Б), "
                    f"вероятно подмена контента. URL: {safe_url}"
                )
                return False
            
            # Проверяем magic bytes
            if not _is_valid_image_content(content[:12]):
                # Определяем что это (HTML? текст?)
                preview = content[:200].decode('utf-8', errors='replace')
                if '<html' in preview.lower() or '<!doctype' in preview.lower():
                    logger.warning(
                        f"Получен HTML вместо изображения (вероятно ISP-перехват). "
                        f"URL: {safe_url}"
                    )
                else:
                    logger.warning(
                        f"Содержимое не является изображением. "
                        f"URL: {safe_url}, начало: {content[:20]!r}"
                    )
                return False
            
            # Все проверки пройдены — сохраняем файл
            with open(file_path, 'wb') as f:
                f.write(content)
            
            logger.debug(f"Скачано: {file_path}")
            return True
            
        except (requests.exceptions.RequestException, requests.exceptions.Timeout, IOError) as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 0.5
                time.sleep(wait_time)
            else:
                logger.error(f"Ошибка при скачивании {safe_url}: {e}")
    
    return False


def get_photo_directory(
    base_path: str,
    series_id: int,
    spec_id: int,
    category_id: int,
    color_id: int,
) -> Path:
    """
    Возвращает путь к директории для сохранения фото.
    
    Структура: IMG/{series_id}/{spec_id}/{category_id}/{color_id}/
    """
    path = Path(base_path) / str(series_id) / str(spec_id) / str(category_id) / str(color_id)
    return path


def get_photo_filename(photo_id: str, url: str, image_type: str = "original") -> str:
    """
    Генерирует имя файла для фото.
    
    Args:
        photo_id: ID фото
        url: URL изображения (для определения расширения)
        image_type: Тип изображения (original, big, small, nowebp)
    
    Returns:
        Имя файла, например: "11353340_original.jpg"
    """
    ext = get_file_extension(url)
    return f"{photo_id}_{image_type}{ext}"


def download_photo(
    photo_id: str,
    series_id: int,
    spec_id: int,
    category_id: int,
    color_id: int,
    url: str,
    base_path: str,
    image_type: str = "original",
    timeout: float = 10.0,
) -> Optional[str]:
    """
    Скачивает одно фото и сохраняет в файловую систему.
    
    Args:
        photo_id: ID фото
        series_id: ID серии
        spec_id: ID комплектации
        category_id: ID категории
        color_id: ID цвета
        url: URL изображения
        base_path: Базовый путь к папке IMG
        image_type: Тип изображения (original, big, small, nowebp)
        timeout: Таймаут запроса
    
    Returns:
        Относительный путь к файлу или None в случае ошибки
    """
    if not url:
        return None
    
    try:
        # Создаем структуру папок
        photo_dir = get_photo_directory(base_path, series_id, spec_id, category_id, color_id)
        
        # Генерируем имя файла
        filename = get_photo_filename(photo_id, url, image_type)
        file_path = photo_dir / filename
        
        # Скачиваем файл
        if download_image(url, file_path, timeout=timeout):
            # Возвращаем относительный путь от base_path
            relative_path = file_path.relative_to(Path(base_path))
            return str(relative_path).replace('\\', '/')  # Нормализуем для кроссплатформенности
        
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке фото {photo_id} ({image_type}): {e}")
        return None


def download_all_photos_for_series(
    series_id: int,
    session_factory,
    base_path: str,
    download_types: list[str] = None,
    timeout: float = 10.0,
    skip_spec_ids: set[int] | None = None,
    only_category_ids: list[int] | None = None,
    progress_callback=None,
) -> dict:
    """
    Загружает все фотографии для указанной серии из БД.
    
    Args:
        series_id: ID серии
        session_factory: Фабрика сессий БД
        base_path: Базовый путь к папке IMG
        download_types: Список типов для загрузки (original, big, small, nowebp). 
                       По умолчанию ['original']
        timeout: Таймаут запроса
        skip_spec_ids: Множество spec_id, которые нужно пропустить (имеют 360 фото)
        only_category_ids: Список ID категорий для загрузки
                          (для спецификаций без 360 фото, при режиме 360only)
    
    Returns:
        Словарь со статистикой: {"downloaded": ..., "skipped": ..., "errors": ...}
    """
    from app.db import session_scope
    from app.models import Photo
    
    if download_types is None:
        download_types = ['original']
    
    downloaded = 0
    skipped = 0
    errors = 0
    
    with session_scope(session_factory) as session:
        query = session.query(Photo).filter(Photo.series_id == series_id)
        
        # Фильтрация по режиму 360only
        if skip_spec_ids:
            query = query.filter(~Photo.specification_id.in_(skip_spec_ids))
        if only_category_ids is not None:
            query = query.filter(Photo.category_id.in_(only_category_ids))
        
        photos = query.all()
        total_photos = len(photos)
        
        # Диагностика: группируем фото по spec_id
        from collections import defaultdict
        photos_by_spec = defaultdict(int)
        for photo in photos:
            photos_by_spec[photo.specification_id] += 1
        
        logger.debug(f"Найдено {total_photos} фото для загрузки (серия {series_id})")
        if photos_by_spec:
            logger.debug(f"Диагностика: фото распределены по {len(photos_by_spec)} комплектациям: {dict(sorted(photos_by_spec.items()))}")
        
        for index, photo in enumerate(photos, 1):
            try:
                # Определяем какие типы загружать
                url_map = {
                    'original': photo.originalpic,


                }
                
                downloaded_any = False
                local_paths = []
                
                for img_type in download_types:
                    url = url_map.get(img_type)
                    if not url:
                        continue
                    
                    local_path = download_photo(
                        photo_id=photo.id,
                        series_id=photo.series_id,
                        spec_id=photo.specification_id,
                        category_id=photo.category_id,
                        color_id=photo.color_id,
                        url=url,
                        base_path=base_path,
                        image_type=img_type,
                        timeout=timeout,
                    )
                    
                    if local_path:
                        local_paths.append(local_path)
                        downloaded_any = True
                
                if downloaded_any:
                    # Сохраняем путь к оригинальному фото в БД (если есть)
                    if 'original' in download_types and photo.originalpic:
                        original_path = next(
                            (p for p in local_paths if 'original' in p),
                            None
                        )
                        if original_path:
                            photo.local_path = original_path
                            session.flush()
                    
                    downloaded += 1
                    if progress_callback:
                        progress_callback("downloaded")
                else:
                    skipped += 1
                    if progress_callback:
                        progress_callback("skipped")
                
                # Показываем прогресс каждые 50 фото
                if index % 50 == 0 or index == total_photos:
                    logger.debug(
                        f"Серия {series_id}: {index}/{total_photos} "
                        f"(↓={downloaded} проп={skipped} ош={errors})"
                    )
                    
            except Exception as e:
                logger.error(f"Ошибка при загрузке фото {photo.id}: {e}")
                errors += 1
                if progress_callback:
                    progress_callback("error")
    
    logger.debug(
        f"Загрузка фото для серии {series_id} завершена: "
        f"скачано: {downloaded}, пропущено: {skipped}, ошибок: {errors}"
    )
    
    return {
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
    }
