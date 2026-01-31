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
    
    Args:
        url: URL изображения
        file_path: Путь для сохранения файла
        timeout: Таймаут запроса
        max_retries: Максимальное количество повторных попыток
    
    Returns:
        True если успешно, False в противном случае
    """
    if file_path.exists():
        logger.debug(f"Файл уже существует: {file_path}")
        return True
    
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
            response = requests.get(url, headers=headers, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Создаем директорию если нужно
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Сохраняем файл
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.debug(f"Скачано: {file_path}")
            return True
            
        except (requests.exceptions.RequestException, requests.exceptions.Timeout, IOError) as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 0.5
                time.sleep(wait_time)
            else:
                logger.error(f"Ошибка при скачивании {url}: {e}")
    
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
        photos = session.query(Photo).filter(Photo.series_id == series_id).all()
        total_photos = len(photos)
        
        logger.info(f"Найдено {total_photos} фото для загрузки (серия {series_id})")
        
        for index, photo in enumerate(photos, 1):
            try:
                # Определяем какие типы загружать
                url_map = {
                    'original': photo.originalpic,
                    'big': photo.bigpic,
                    'small': photo.smallpic,
                    'nowebp': photo.nowebppic,
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
                        spec_id=photo.spec_id,
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
                else:
                    skipped += 1
                
                # Показываем прогресс каждые 10 фото или на важных этапах
                if index % 10 == 0 or index == 1 or index == total_photos:
                    logger.info(
                        f"Прогресс: {index}/{total_photos} "
                        f"(скачано: {downloaded}, пропущено: {skipped}, ошибок: {errors})"
                    )
                # Дополнительное логирование каждые 50 загруженных фото
                elif downloaded > 0 and downloaded % 50 == 0:
                    logger.info(
                        f"  ✓ Загружено {downloaded} фото "
                        f"({index}/{total_photos} обработано)"
                    )
                    
            except Exception as e:
                logger.error(f"Ошибка при загрузке фото {photo.id}: {e}")
                errors += 1
                # Показываем прогресс даже при ошибке
                if index % 10 == 0 or index == total_photos:
                    logger.info(
                        f"Прогресс: {index}/{total_photos} "
                        f"(скачано: {downloaded}, пропущено: {skipped}, ошибок: {errors})"
                    )
    
    logger.info(
        f"Загрузка фото для серии {series_id} завершена: "
        f"скачано: {downloaded}, пропущено: {skipped}, ошибок: {errors}"
    )
    
    return {
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
    }
