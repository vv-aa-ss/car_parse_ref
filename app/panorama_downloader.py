"""
Модуль для загрузки 360-градусных фото в файловую систему.
"""
from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from pathlib import Path
from typing import Optional

import requests

from app.db import session_scope
from app.models import PanoramaPhoto, Spec
from app.photo_downloader import download_image, get_file_extension

logger = logging.getLogger(__name__)


def get_panorama_directory(
    base_path: str,
    series_id: int,
    spec_id: int,
    color_id: int,
) -> Path:
    """
    Возвращает путь к директории для сохранения 360 фото.
    
    Структура: IMG/{series_id}/{spec_id}/360/{color_id}/
    """
    path = Path(base_path) / str(series_id) / str(spec_id) / "360" / str(color_id)
    return path


def get_panorama_filename(photo_id: str, url: str, seq: int) -> str:
    """
    Генерирует имя файла для 360 фото.
    
    Args:
        photo_id: ID фото из БД
        url: URL изображения (для определения расширения)
        seq: Порядковый номер кадра
    
    Returns:
        Имя файла, например: "72006_13349_0_hori_l1.png"
    """
    ext = get_file_extension(url)
    # Используем seq для имени файла, чтобы было понятно порядок кадров
    return f"{seq:03d}{ext}"  # Формат: 000.png, 001.png, 002.png и т.д.


def download_panorama_photo(
    photo_id: str,
    series_id: int,
    spec_id: int,
    color_id: int,
    seq: int,
    url: str,
    base_path: str,
    timeout: float = 10.0,
) -> Optional[str]:
    """
    Скачивает одно 360 фото и сохраняет в файловую систему.
    
    Args:
        photo_id: ID фото из БД
        series_id: ID серии
        spec_id: ID комплектации
        color_id: ID цвета
        seq: Порядковый номер кадра
        url: URL изображения
        base_path: Базовый путь к папке IMG
        timeout: Таймаут запроса
    
    Returns:
        Относительный путь к файлу или None в случае ошибки
    """
    if not url:
        return None
    
    try:
        # Создаем структуру папок: IMG/{series_id}/{spec_id}/360/{color_id}/
        photo_dir = get_panorama_directory(base_path, series_id, spec_id, color_id)
        
        # Генерируем имя файла
        filename = get_panorama_filename(photo_id, url, seq)
        file_path = photo_dir / filename
        
        # Скачиваем файл
        if download_image(url, file_path, timeout=timeout):
            # Возвращаем относительный путь от base_path
            relative_path = file_path.relative_to(Path(base_path))
            return str(relative_path).replace('\\', '/')  # Нормализуем для кроссплатформенности
        
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке 360 фото {photo_id} (seq {seq}): {e}")
        return None


def _download_single_panorama_photo(
    photo: PanoramaPhoto,
    series_id: int,
    base_path: str,
    timeout: float,
    stats: dict,
    lock: threading.Lock,
    session_factory,
    progress_callback=None,
) -> None:
    """
    Загружает одно 360 фото (используется в параллельной загрузке).
    
    Args:
        photo: Объект PanoramaPhoto из БД
        series_id: ID серии
        base_path: Базовый путь к папке IMG
        timeout: Таймаут запроса
        stats: Словарь со статистикой (downloaded, skipped, errors)
        lock: Блокировка для потокобезопасного обновления статистики
        session_factory: Фабрика сессий БД для обновления local_path
    """
    try:
        # Пропускаем, если уже скачано
        if photo.local_path:
            # photo.local_path может быть относительным путем с / или \
            # Нормализуем его и проверяем существование
            local_path_normalized = str(photo.local_path).replace('\\', '/')
            file_path = Path(base_path) / local_path_normalized
            if file_path.exists():
                with lock:
                    stats["skipped"] += 1
                if progress_callback:
                    progress_callback("skipped")
                return
        
        # Скачиваем фото
        local_path = download_panorama_photo(
            photo_id=photo.id,
            series_id=series_id,
            spec_id=photo.spec_id,
            color_id=photo.color_id,
            seq=photo.seq,
            url=photo.url,
            base_path=base_path,
            timeout=timeout,
        )
        
        if local_path:
            # Обновляем local_path в БД
            with session_scope(session_factory) as session:
                photo_obj = session.query(PanoramaPhoto).filter(PanoramaPhoto.id == photo.id).first()
                if photo_obj:
                    photo_obj.local_path = local_path
                    session.commit()
            
            with lock:
                stats["downloaded"] += 1
            if progress_callback:
                progress_callback("downloaded")
        else:
            with lock:
                stats["errors"] += 1
            if progress_callback:
                progress_callback("error")
                
    except Exception as e:
        logger.error(f"Ошибка при загрузке 360 фото {photo.id}: {e}")
        with lock:
            stats["errors"] += 1
        if progress_callback:
            progress_callback("error")


def download_all_panorama_photos_for_spec(
    spec_id: int,
    session_factory,
    base_path: str,
    timeout: float = 10.0,
    max_workers: int = 10,
    progress_callback=None,
) -> dict:
    """
    Загружает все 360 фото для указанной комплектации в файловую систему.
    Использует параллельную загрузку для ускорения процесса.
    
    Args:
        spec_id: ID комплектации
        session_factory: Фабрика сессий БД
        base_path: Базовый путь к папке IMG
        timeout: Таймаут запроса
        max_workers: Количество потоков для параллельной загрузки (по умолчанию 10)
    
    Returns:
        Словарь со статистикой: {"downloaded": ..., "skipped": ..., "errors": ...}
    """
    stats = {
        "downloaded": 0,
        "skipped": 0,
        "errors": 0,
    }
    lock = threading.Lock()
    
    with session_scope(session_factory) as session:
        # Получаем series_id из spec
        spec = session.query(Spec).filter(Spec.id == spec_id).first()
        if not spec:
            logger.error(f"Комплектация {spec_id} не найдена в БД")
            return {"downloaded": 0, "skipped": 0, "errors": 0}
        
        series_id = spec.series_id
        
        # Получаем все 360 фото для этой комплектации
        photos = session.query(PanoramaPhoto).filter(PanoramaPhoto.spec_id == spec_id).all()
        total_photos = len(photos)
        
        logger.info(f"Найдено {total_photos} фото 360 для загрузки (spec_id={spec_id}, series_id={series_id})")
        
        if total_photos == 0:
            logger.warning(f"Нет фото 360 для комплектации {spec_id}")
            return {"downloaded": 0, "skipped": 0, "errors": 0}
        
        # Параллельная загрузка фото
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _download_single_panorama_photo,
                    photo,
                    series_id,
                    base_path,
                    timeout,
                    stats,
                    lock,
                    session_factory,
                    progress_callback,
                ): photo
                for photo in photos
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка при загрузке фото: {e}")
                    with lock:
                        stats["errors"] += 1
                
                # Логируем прогресс каждые 100 фото
                if completed % 100 == 0 or completed == total_photos:
                    with lock:
                        logger.debug(
                            f"Spec {spec_id}: {completed}/{total_photos} | "
                            f"↓={stats['downloaded']} проп={stats['skipped']} ош={stats['errors']}"
                        )
    
    logger.debug(
        f"Загрузка 360 фото для spec_id={spec_id} завершена: "
        f"скачано: {stats['downloaded']}, пропущено: {stats['skipped']}, ошибок: {stats['errors']}"
    )
    
    return {
        "downloaded": stats["downloaded"],
        "skipped": stats["skipped"],
        "errors": stats["errors"],
    }
