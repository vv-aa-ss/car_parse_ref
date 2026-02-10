from __future__ import annotations

import concurrent.futures
import logging
import logging.handlers
import sys
import threading
import time
from typing import Iterable, List

from pathlib import Path
from tqdm import tqdm

from app.config import load_settings, Settings
from app.db import create_db_engine, create_session_factory, ensure_database_exists, session_scope
from app.models import Base, Photo, PhotoColor, PhotoCategory, PanoramaPhoto, Spec
from app.parser.autohome_client import AutohomeClient
from app.parser.parsers import limit_series_per_brand, parse_param_conf, parse_tree_menu
from app.panorama_downloader import download_all_panorama_photos_for_spec
from app.panorama_parser import parse_panorama_colors, parse_panorama_photos
from app.photo_downloader import download_all_photos_for_series
from app.photo_parser import parse_all_photos, parse_foto
from app.repository import (
    upsert_brands,
    upsert_param_titles,
    upsert_param_values,
    upsert_series,
    upsert_specs,
)


# ---------------------------------------------------------------------------
#  Вывод в терминал — через tqdm.write, чтобы не ломать прогресс-бары
# ---------------------------------------------------------------------------

def _print(msg: str = "") -> None:
    """Печатает строку в терминал (совместимо с tqdm)."""
    tqdm.write(msg, file=sys.stderr)


def _setup_logging(settings: Settings) -> None:
    """
    Все логи (DEBUG+) пишутся ТОЛЬКО в файл с ротацией по дням.
    В терминале ничего не выводится через logging —
    ключевая информация выводится через _print() / tqdm.
    """
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "parser.log"

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=settings.log_retention_days,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    root = logging.getLogger()
    root.handlers = [file_handler]
    root.setLevel(logging.DEBUG)

    # Подавляем шумные библиотеки
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def _make_pbar(total: int, desc: str, unit: str, colour: str | None = None) -> tqdm:
    """Создаёт трёхстрочный прогресс-бар.

    Строка 1:  Описание id=XXXX | статистика  NN%
    Строка 2:  n/total [elapsed<remaining, rate]
    Строка 3:  |████████████░░░░░░░░░░░|
    """
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        colour=colour,
        dynamic_ncols=True,
        bar_format=(
            "{desc} {percentage:3.0f}%\n"
            "{n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]\n"
            "|{bar:50}|"
        ),
        file=sys.stderr,
    )


def _filter_series_for_reparse(
    session_factory, series_ids: Iterable[int], force: bool
) -> List[int]:
    if force:
        return list(series_ids)
    filtered: List[int] = []
    with session_scope(session_factory) as session:
        for series_id in series_ids:
            exists = (
                session.query(Spec.id)
                .filter(Spec.series_id == series_id)
                .limit(1)
                .first()
                is not None
            )
            if not exists:
                filtered.append(series_id)
    return filtered


def _parse_and_store_series(
    series_id: int, timeout: float, session_factory, progress_counter: dict,
    parse_modes: list[int] | None = None,
) -> None:
    if parse_modes is None:
        parse_modes = [1]
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            all_titles = []
            all_specs = []
            all_param_values = []
            seen_spec_ids: set[int] = set()
            
            with AutohomeClient(timeout=timeout) as client:
                for mode in parse_modes:
                    payload = client.get_param_conf(series_id, mode=mode)
                    parsed = parse_param_conf(payload, series_id)
                    
                    all_titles.extend(parsed["titles"])
                    
                    # Дедупликация specs и param_values по spec_id
                    for spec in parsed["specs"]:
                        if spec.id not in seen_spec_ids:
                            seen_spec_ids.add(spec.id)
                            all_specs.append(spec)
                    
                    all_param_values.extend(parsed["param_values"])
            
            with session_scope(session_factory) as session:
                titles_stats = upsert_param_titles(session, all_titles)
                specs_stats = upsert_specs(session, all_specs)
                values_stats = upsert_param_values(session, all_param_values)
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["specs_inserted"] += specs_stats["inserted"]
                progress_counter["specs_updated"] += specs_stats["updated"]
                progress_counter["specs_skipped"] += specs_stats["skipped"]
                progress_counter["values_inserted"] += values_stats["inserted"]
                progress_counter["values_updated"] += values_stats["updated"]
            return  # Успешно обработано
            
        except Exception as exc:
            error_msg = str(exc).split("\n")[0]
            is_deadlock = "DeadlockDetected" in error_msg or "deadlock" in error_msg.lower()
            
            if attempt < max_retries - 1 and is_deadlock:
                # Retry для deadlock ошибок
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            
            # Финальная ошибка или не deadlock
            with progress_counter["lock"]:
                progress_counter["errors"] += 1
                errors = progress_counter["errors"]
            logging.error(
                "Ошибка при парсинге серии %d (попытка %d/%d): %s (всего ошибок: %d)",
                series_id,
                attempt + 1,
                max_retries,
                error_msg,
                errors,
            )
            raise  # Пробрасываем ошибку дальше


def _parse_photos_for_series(
    series_id: int,
    timeout: float,
    session_factory,
    page_size: int,
    max_combinations: int,
    progress_counter: dict,
    skip_spec_ids: set[int] | None = None,
    only_category_ids: list[int] | None = None,
    max_colors: int = 0,
) -> None:
    """
    Парсит фото для указанной серии (цвета, категории, ссылки на фото).
    """
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            with AutohomeClient(timeout=timeout) as client:
                # Сначала получаем цвета и категории
                parse_foto(series_id, session_factory, client)
                
                # Затем парсим все фото
                photos_stats = parse_all_photos(
                    series_id=series_id,
                    session_factory=session_factory,
                    client=client,
                    page_size=page_size,
                    max_combinations=max_combinations,
                    skip_spec_ids=skip_spec_ids,
                    only_category_ids=only_category_ids,
                    max_colors=max_colors,
                )
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["photos_inserted"] += photos_stats["inserted"]
                progress_counter["photos_updated"] += photos_stats["updated"]
                progress_counter["photos_skipped"] += photos_stats["skipped"]
                progress_counter["photos_errors"] += photos_stats["errors"]
            return  # Успешно обработано
            
        except Exception as exc:
            error_msg = str(exc).split("\n")[0]
            is_deadlock = "DeadlockDetected" in error_msg or "deadlock" in error_msg.lower()
            
            if attempt < max_retries - 1 and is_deadlock:
                # Retry для deadlock ошибок
                time.sleep(retry_delay * (attempt + 1))
                continue
            
            # Финальная ошибка или не deadlock
            with progress_counter["lock"]:
                progress_counter["photos_errors"] += 1
                errors = progress_counter["photos_errors"]
            logging.error(
                "Ошибка при парсинге фото для серии %d (попытка %d/%d): %s (всего ошибок: %d)",
                series_id,
                attempt + 1,
                max_retries,
                error_msg,
                errors,
            )
            if attempt == max_retries - 1:
                return  # Не пробрасываем ошибку, чтобы не останавливать весь процесс


def _download_photos_for_series(
    series_id: int,
    session_factory,
    base_path: str,
    timeout: float,
    progress_counter: dict,
    skip_spec_ids: set[int] | None = None,
    only_category_ids: list[int] | None = None,
) -> None:
    """
    Загружает фото для указанной серии в файловую систему.
    """
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            result = download_all_photos_for_series(
                series_id=series_id,
                session_factory=session_factory,
                base_path=base_path,
                download_types=['original'],
                timeout=timeout,
                skip_spec_ids=skip_spec_ids,
                only_category_ids=only_category_ids,
            )
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["downloaded"] += result["downloaded"]
                progress_counter["skipped"] += result["skipped"]
                progress_counter["download_errors"] += result["errors"]
            return  # Успешно обработано
            
        except Exception as exc:
            error_msg = str(exc).split("\n")[0]
            is_deadlock = "DeadlockDetected" in error_msg or "deadlock" in error_msg.lower()
            
            if attempt < max_retries - 1 and is_deadlock:
                # Retry для deadlock ошибок
                time.sleep(retry_delay * (attempt + 1))
                continue
            
            # Финальная ошибка или не deadlock
            with progress_counter["lock"]:
                progress_counter["download_errors"] += 1
                errors = progress_counter["download_errors"]
            logging.error(
                "Ошибка при загрузке фото для серии %d (попытка %d/%d): %s (всего ошибок: %d)",
                series_id,
                attempt + 1,
                max_retries,
                error_msg,
                errors,
            )
            if attempt == max_retries - 1:
                return  # Не пробрасываем ошибку, чтобы не останавливать весь процесс


def _parse_panoramas_for_spec(
    spec_id: int,
    timeout: float,
    session_factory,
    progress_counter: dict,
) -> None:
    """
    Парсит 360 фото для указанной комплектации (цвета и фото).
    """
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            with AutohomeClient(timeout=timeout) as client:
                # Парсим цвета и фото из baseinfo
                colors_result = parse_panorama_colors(
                    spec_id=spec_id,
                    session_factory=session_factory,
                    client=client,
                    ext_id=None,  # Автоматический поиск ext_id
                )
                
                # Проверяем, были ли найдены цвета (если нет - значит нет фото 360 для этой комплектации)
                colors_found = (colors_result.get("colors", {}).get("inserted", 0) > 0 or 
                               colors_result.get("colors", {}).get("updated", 0) > 0)
                
                if colors_found:
                    # Если цвета найдены, парсим дополнительные фото через getVrInfo
                    # (если фото не были получены из baseinfo)
                    photos_from_baseinfo = colors_result.get("photos", {}).get("inserted", 0)
                    if photos_from_baseinfo == 0:
                        try:
                            photos_result = parse_panorama_photos(
                                spec_id=spec_id,
                                session_factory=session_factory,
                                client=client,
                            )
                            colors_result["photos"] = photos_result
                        except Exception as e:
                            # Если не удалось спарсить фото через getVrInfo - это не критично
                            logging.debug(f"Не удалось спарсить фото 360 через getVrInfo для spec_id {spec_id}: {e}")
                            # Оставляем фото из baseinfo (если они были)
                else:
                    # Нет цветов = нет фото 360 для этой комплектации
                    # Это нормально, не считаем ошибкой
                    pass
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["colors_inserted"] += colors_result.get("colors", {}).get("inserted", 0)
                progress_counter["colors_updated"] += colors_result.get("colors", {}).get("updated", 0)
                progress_counter["panorama_photos_inserted"] += colors_result.get("photos", {}).get("inserted", 0)
                progress_counter["panorama_photos_updated"] += colors_result.get("photos", {}).get("updated", 0)
                progress_counter["panorama_photos_skipped"] += colors_result.get("photos", {}).get("skipped", 0)
            return  # Успешно обработано
            
        except Exception as exc:
            error_msg = str(exc).split("\n")[0]
            is_deadlock = "DeadlockDetected" in error_msg or "deadlock" in error_msg.lower()
            
            # Если не удалось найти ext_id или нет фото 360 - это нормально, не считаем ошибкой
            is_no_panorama = (
                "Не удалось найти ext_id" in error_msg or 
                "не найдено цветов" in error_msg.lower() or
                "не найдено цветов для 360 фото" in error_msg.lower()
            )
            
            if is_no_panorama:
                # Нет фото 360 для этой комплектации - это нормально
                with progress_counter["lock"]:
                    progress_counter["parsed"] += 1
                    progress_counter["skipped_no_panorama"] += 1
                logging.debug(f"Нет фото 360 для spec_id {spec_id} (это нормально)")
                return
            
            if attempt < max_retries - 1 and is_deadlock:
                # Retry для deadlock ошибок
                time.sleep(retry_delay * (attempt + 1))
                continue
            
            # Финальная ошибка или не deadlock
            with progress_counter["lock"]:
                progress_counter["errors"] += 1
                errors = progress_counter["errors"]
            logging.error(
                "Ошибка при парсинге 360 фото для spec_id %d (попытка %d/%d): %s (всего ошибок: %d)",
                spec_id,
                attempt + 1,
                max_retries,
                error_msg,
                errors,
            )
            if attempt == max_retries - 1:
                return  # Не пробрасываем ошибку, чтобы не останавливать весь процесс


def _download_panoramas_for_spec(
    spec_id: int,
    session_factory,
    base_path: str,
    timeout: float,
    progress_counter: dict,
    max_workers: int = 10,
) -> None:
    """
    Загружает 360 фото для указанной комплектации в файловую систему.
    """
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            result = download_all_panorama_photos_for_spec(
                spec_id=spec_id,
                session_factory=session_factory,
                base_path=base_path,
                timeout=timeout,
                max_workers=max_workers,
            )
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["downloaded"] += result["downloaded"]
                progress_counter["skipped"] += result["skipped"]
                progress_counter["download_errors"] += result["errors"]
            return  # Успешно обработано
            
        except Exception as exc:
            error_msg = str(exc).split("\n")[0]
            is_deadlock = "DeadlockDetected" in error_msg or "deadlock" in error_msg.lower()
            
            if attempt < max_retries - 1 and is_deadlock:
                # Retry для deadlock ошибок
                time.sleep(retry_delay * (attempt + 1))
                continue
            
            # Финальная ошибка или не deadlock
            with progress_counter["lock"]:
                progress_counter["download_errors"] += 1
                errors = progress_counter["download_errors"]
            logging.error(
                "Ошибка при загрузке 360 фото для spec_id %d (попытка %d/%d): %s (всего ошибок: %d)",
                spec_id,
                attempt + 1,
                max_retries,
                error_msg,
                errors,
            )
            if attempt == max_retries - 1:
                return  # Не пробрасываем ошибку, чтобы не останавливать весь процесс


def main() -> None:
    settings = load_settings()
    _setup_logging(settings)

    logging.info("=" * 60)
    logging.info("Запуск парсера автомобилей Autohome")
    logging.info("=" * 60)
    logging.info("Настройки: MODELS_PER_BRAND=%d, PARSE_WORKERS=%d, PARSE_MODES=%s, PAGESIZE=%d, MAX_COLORS=%d",
                 settings.models_per_brand, settings.parse_workers, settings.parse_modes,
                 settings.pagesize, settings.max_colors)

    _print("=" * 60)
    _print("  Autohome Parser")
    _print(f"  Workers={settings.parse_workers}  Modes={settings.parse_modes}  "
           f"PageSize={settings.pagesize}  MaxColors={settings.max_colors}")
    if settings.photo_360_only:
        logging.info("Режим 360ONLY включен! Категории фото для машин без 360: %s",
                     settings.photo_360_only_categories)
        _print(f"  360ONLY=on  Категории: {settings.photo_360_only_categories}")
    _print(f"  Логи → {Path(settings.log_dir).resolve() / 'parser.log'}")
    _print("=" * 60)

    logging.info("Загрузка дерева брендов и моделей...")
    _print("⏳ Загрузка дерева брендов…")
    with AutohomeClient(timeout=settings.api_timeout) as client:
        tree_payload = client.get_tree_menu()

    parsed_tree = parse_tree_menu(tree_payload)

    logging.info("Получено из API: %d брендов, %d серий",
                 len(parsed_tree["brands"]), len(parsed_tree["series"]))

    # Применяем лимит количества брендов
    if settings.models_per_brand > 0:
        logging.info("Применяется лимит: %d брендов (со всеми их моделями)", settings.models_per_brand)
        series_limited = limit_series_per_brand(
            parsed_tree["series"], settings.models_per_brand
        )
        limited_brand_ids = set(s.brand_id for s in series_limited)
        logging.info("После применения лимита: %d брендов, %d серий (было %d брендов, %d серий)",
                     len(limited_brand_ids), len(series_limited),
                     len(parsed_tree["brands"]), len(parsed_tree["series"]))
        _print(f"  Брендов: {len(limited_brand_ids)}, серий: {len(series_limited)}")
    else:
        series_limited = parsed_tree["series"]
        logging.info("Лимит брендов не применен (MODELS_PER_BRAND=0 или не указан)")
        _print(f"  Брендов: {len(parsed_tree['brands'])}, серий: {len(parsed_tree['series'])}")

    # Сохраняем исходный список серий для парсинга фото (до фильтрации FORCE_REPARSE)
    original_series_ids = [series.id for series in series_limited]
    series_ids = original_series_ids.copy()

    if not settings.use_database:
        logging.info("Режим без БД: данные не сохраняются")
        _print("⚠ Режим без БД — выход.")
        return

    logging.info("Проверка и создание базы данных...")
    ensure_database_exists(settings)
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(engine)

    logging.info("Сохранение брендов и серий в БД...")
    with session_scope(session_factory) as session:
        brands_stats = upsert_brands(session, parsed_tree["brands"])
        logging.info("Бренды: добавлено %d, обновлено %d, пропущено %d",
                     brands_stats["inserted"], brands_stats["updated"], brands_stats["skipped"])
        series_stats = upsert_series(session, series_limited)
        logging.info("Серии: добавлено %d, обновлено %d, пропущено %d",
                     series_stats["inserted"], series_stats["updated"], series_stats["skipped"])

    logging.info("Фильтрация серий для парсинга...")
    series_ids = _filter_series_for_reparse(
        session_factory, series_ids, settings.force_reparse
    )

    if not series_ids:
        logging.info("Нет новых серий для парсинга.")
        _print("✔ Нет новых серий для парсинга.")
        return

    logging.info("Найдено %d серий для парсинга характеристик", len(series_ids))
    logging.info("Используется %d потоков для параллельного парсинга", settings.parse_workers)
    logging.info("-" * 60)

    # Счетчик прогресса с блокировкой для потокобезопасности
    progress_counter = {
        "lock": threading.Lock(),
        "total": len(series_ids),
        "parsed": 0,
        "errors": 0,
        "specs_inserted": 0,
        "specs_updated": 0,
        "specs_skipped": 0,
        "values_inserted": 0,
        "values_updated": 0,
    }

    logging.info("Запуск парсинга в %d потоках...", settings.parse_workers)
    _print()
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=settings.parse_workers
    ) as executor:
        futures = {
            executor.submit(
                _parse_and_store_series,
                series_id,
                settings.api_timeout,
                session_factory,
                progress_counter,
                settings.parse_modes,
            ): series_id
            for series_id in series_ids
        }

        with _make_pbar(len(series_ids), "Характеристики", "сер", colour="green") as pbar:
            for future in concurrent.futures.as_completed(futures):
                sid = futures[future]
                try:
                    future.result()
                except Exception:
                    pass
                with progress_counter["lock"]:
                    pbar.set_description_str(
                        f"Характеристики сер={sid} | "
                        f"+спек={progress_counter['specs_inserted']} "
                        f"обн={progress_counter['specs_updated']} "
                        f"ош={progress_counter['errors']}"
                    )
                pbar.update(1)

    elapsed_time = time.time() - start_time
    logging.info("Парсинг завершён за %.1f сек (%.2f сек/серия) | "
                 "Спецификации: +%d обн %d | Параметры: +%d обн %d | Ошибок: %d",
                 elapsed_time,
                 elapsed_time / max(progress_counter["parsed"], 1),
                 progress_counter["specs_inserted"],
                 progress_counter["specs_updated"],
                 progress_counter["values_inserted"],
                 progress_counter["values_updated"],
                 progress_counter["errors"])
    logging.info("=" * 60)
    
    # ================================================================
    # Определяем порядок этапов в зависимости от режима 360only
    # Когда 360only включен, парсинг панорам идет ДО обычных фото,
    # чтобы определить, у каких спецификаций есть 360 фото
    # ================================================================
    
    panoramas_already_parsed = False  # Флаг: были ли панорамы уже спарсены
    specs_with_panorama: set[int] = set()  # Множество spec_id с 360 фото
    
    # Итоговые счётчики для финального отчёта (инициализируем рано, т.к. 360only может обновить их)
    summary = {
        "photos_added": 0,
        "photos_updated": 0,
        "photos_downloaded": 0,
        "photos_download_skipped": 0,
        "panorama_colors": 0,
        "panorama_photos": 0,
        "panorama_downloaded": 0,
        "panorama_download_skipped": 0,
    }
    
    # Если 360only включен — парсим панорамы ПЕРВЫМИ
    if settings.photo_360_only and settings.parse_panoramas:
        logging.info("360ONLY: Парсинг 360-градусных фото (панорам) — ПЕРВЫЙ ЭТАП")

        with session_scope(session_factory) as session:
            specs = session.query(Spec.id).filter(Spec.series_id.in_(series_ids)).all()
            spec_ids = [s[0] for s in specs]

        if not spec_ids:
            logging.info("Нет комплектаций для парсинга 360 фото.")
        else:
            logging.info("Найдено %d комплектаций для парсинга 360 фото", len(spec_ids))

            panorama_progress = {
                "lock": threading.Lock(),
                "total": len(spec_ids),
                "parsed": 0,
                "colors_inserted": 0,
                "colors_updated": 0,
                "panorama_photos_inserted": 0,
                "panorama_photos_updated": 0,
                "panorama_photos_skipped": 0,
                "skipped_no_panorama": 0,
                "errors": 0,
            }

            logging.info("Запуск парсинга 360 фото в %d потоках...", settings.parse_workers)
            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.parse_workers
            ) as executor:
                futures = {
                    executor.submit(
                        _parse_panoramas_for_spec,
                        spec_id,
                        settings.api_timeout,
                        session_factory,
                        panorama_progress,
                    ): spec_id
                    for spec_id in spec_ids
                }

                with _make_pbar(len(spec_ids), "Парсинг 360 (1)", "спек", colour="yellow") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        sid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        with panorama_progress["lock"]:
                            pbar.set_description_str(
                                f"Парсинг 360 (1) спек={sid} | "
                                f"+цв={panorama_progress['colors_inserted']} "
                                f"+фото={panorama_progress['panorama_photos_inserted']} "
                                f"ош={panorama_progress['errors']}"
                            )
                        pbar.update(1)

            elapsed_time = time.time() - start_time
            logging.info("Парсинг 360 завершён за %.1f сек (%.2f сек/спек) | "
                         "Цвета: +%d | Фото: +%d | Без 360: %d | Ошибок: %d",
                         elapsed_time,
                         elapsed_time / max(panorama_progress["parsed"], 1),
                         panorama_progress["colors_inserted"],
                         panorama_progress["panorama_photos_inserted"],
                         panorama_progress["skipped_no_panorama"],
                         panorama_progress["errors"])
            summary["panorama_colors"] += panorama_progress["colors_inserted"]
            summary["panorama_photos"] += panorama_progress["panorama_photos_inserted"]

        panoramas_already_parsed = True

    # Если 360only включен — определяем, у каких спецификаций есть 360 фото
    if settings.photo_360_only:
        from app.models import PanoramaColor
        with session_scope(session_factory) as session:
            panorama_specs = session.query(PanoramaColor.spec_id).distinct().all()
            specs_with_panorama = {s[0] for s in panorama_specs}

        logging.info("360ONLY: найдено %d комплектаций с 360 фото", len(specs_with_panorama))
        if specs_with_panorama:
            logging.info("360ONLY: для комплектаций БЕЗ 360 фото будут загружаться только категории: %s",
                         settings.photo_360_only_categories)
    
    # Парсинг фото (если включен)
    if settings.parse_photos:
        logging.info("Начало парсинга фотографий")

        with session_scope(session_factory) as session:
            series_with_specs = session.query(Spec.series_id).distinct().all()
            all_series_with_specs = {s[0] for s in series_with_specs}
            series_ids_for_photos = [s for s in original_series_ids if s in all_series_with_specs]

        if not series_ids_for_photos:
            logging.info("Нет серий с комплектациями для парсинга фото.")
        else:
            logging.info("Найдено %d серий для парсинга фото", len(series_ids_for_photos))

            photos_progress = {
                "lock": threading.Lock(),
                "total": len(series_ids_for_photos),
                "parsed": 0,
                "photos_inserted": 0,
                "photos_updated": 0,
                "photos_skipped": 0,
                "photos_errors": 0,
            }

            photo_skip_spec_ids = specs_with_panorama if settings.photo_360_only else None
            photo_only_category_ids = settings.photo_360_only_categories if settings.photo_360_only else None

            logging.info("Запуск парсинга фото в %d потоках...", settings.parse_workers)
            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.parse_workers
            ) as executor:
                futures = {
                    executor.submit(
                        _parse_photos_for_series,
                        series_id,
                        settings.api_timeout,
                        session_factory,
                        settings.pagesize,
                        settings.max_photo_combinations,
                        photos_progress,
                        photo_skip_spec_ids,
                        photo_only_category_ids,
                        settings.max_colors,
                    ): series_id
                    for series_id in series_ids_for_photos
                }

                with _make_pbar(len(series_ids_for_photos), "Парсинг фото", "сер", colour="blue") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        sid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        with photos_progress["lock"]:
                            elapsed = time.time() - start_time
                            total_photos = photos_progress["photos_inserted"] + photos_progress["photos_updated"]
                            speed = total_photos / elapsed if elapsed > 0 else 0
                            pbar.set_description_str(
                                f"Парсинг фото сер={sid} | "
                                f"+фото={photos_progress['photos_inserted']} "
                                f"обн={photos_progress['photos_updated']} "
                                f"({speed:.0f} ф/с) "
                                f"ош={photos_progress['photos_errors']}"
                            )
                        pbar.update(1)

            elapsed_time = time.time() - start_time
            logging.info("Парсинг фото завершён за %.1f сек (%.2f сек/серия) | "
                         "Фото: +%d обн %d проп %d | Ошибок: %d",
                         elapsed_time,
                         elapsed_time / max(photos_progress["parsed"], 1),
                         photos_progress["photos_inserted"],
                         photos_progress["photos_updated"],
                         photos_progress["photos_skipped"],
                         photos_progress["photos_errors"])
            summary["photos_added"] = photos_progress["photos_inserted"]
            summary["photos_updated"] = photos_progress["photos_updated"]
    
    # Загрузка фото (если включена)
    if settings.download_photos:
        logging.info("Начало загрузки фотографий в файловую систему")

        with session_scope(session_factory) as session:
            series_with_photos = session.query(Photo.series_id).distinct().all()
            series_ids_for_download = [s[0] for s in series_with_photos if s[0] in series_ids]

        if not series_ids_for_download:
            logging.info("Нет серий с фото для загрузки.")
        else:
            img_path = Path(settings.img_path)
            img_path.mkdir(parents=True, exist_ok=True)

            logging.info("Найдено %d серий для загрузки фото → %s", len(series_ids_for_download), img_path.absolute())

            download_progress = {
                "lock": threading.Lock(),
                "total": len(series_ids_for_download),
                "parsed": 0,
                "downloaded": 0,
                "skipped": 0,
                "download_errors": 0,
            }

            dl_skip_spec_ids = specs_with_panorama if settings.photo_360_only else None
            dl_only_category_ids = settings.photo_360_only_categories if settings.photo_360_only else None

            logging.info("Запуск загрузки фото в %d потоках...", settings.parse_workers)
            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.parse_workers
            ) as executor:
                futures = {
                    executor.submit(
                        _download_photos_for_series,
                        series_id,
                        session_factory,
                        str(img_path),
                        settings.api_timeout * 3,
                        download_progress,
                        dl_skip_spec_ids,
                        dl_only_category_ids,
                    ): series_id
                    for series_id in series_ids_for_download
                }

                with _make_pbar(len(series_ids_for_download), "Загрузка фото", "сер", colour="cyan") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        sid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        with download_progress["lock"]:
                            elapsed = time.time() - start_time
                            dl = download_progress["downloaded"]
                            speed = dl / elapsed if elapsed > 0 else 0
                            pbar.set_description_str(
                                f"Загрузка фото сер={sid} | "
                                f"↓={dl} проп={download_progress['skipped']} "
                                f"({speed:.1f} ф/с) "
                                f"ош={download_progress['download_errors']}"
                            )
                        pbar.update(1)

            elapsed_time = time.time() - start_time
            logging.info("Загрузка фото завершена за %.1f сек (%.2f сек/серия) | "
                         "Скачано: %d, пропущено: %d | Ошибок: %d",
                         elapsed_time,
                         elapsed_time / max(download_progress["parsed"], 1),
                         download_progress["downloaded"],
                         download_progress["skipped"],
                         download_progress["download_errors"])
            summary["photos_downloaded"] = download_progress["downloaded"]
            summary["photos_download_skipped"] = download_progress["skipped"]
    
    # Парсинг 360 фото (если включен и ещё не был выполнен ранее в режиме 360only)
    if settings.parse_panoramas and not panoramas_already_parsed:
        logging.info("Начало парсинга 360-градусных фото (панорам)")

        with session_scope(session_factory) as session:
            specs = session.query(Spec.id).filter(Spec.series_id.in_(series_ids)).all()
            spec_ids = [s[0] for s in specs]

        if not spec_ids:
            logging.info("Нет комплектаций для парсинга 360 фото.")
        else:
            logging.info("Найдено %d комплектаций для парсинга 360 фото", len(spec_ids))

            panorama_progress = {
                "lock": threading.Lock(),
                "total": len(spec_ids),
                "parsed": 0,
                "colors_inserted": 0,
                "colors_updated": 0,
                "panorama_photos_inserted": 0,
                "panorama_photos_updated": 0,
                "panorama_photos_skipped": 0,
                "skipped_no_panorama": 0,
                "errors": 0,
            }

            logging.info("Запуск парсинга 360 фото в %d потоках...", settings.parse_workers)
            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.parse_workers
            ) as executor:
                futures = {
                    executor.submit(
                        _parse_panoramas_for_spec,
                        spec_id,
                        settings.api_timeout,
                        session_factory,
                        panorama_progress,
                    ): spec_id
                    for spec_id in spec_ids
                }

                with _make_pbar(len(spec_ids), "Парсинг 360", "спек", colour="yellow") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        sid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        with panorama_progress["lock"]:
                            pbar.set_description_str(
                                f"Парсинг 360 спек={sid} | "
                                f"+цв={panorama_progress['colors_inserted']} "
                                f"+фото={panorama_progress['panorama_photos_inserted']} "
                                f"ош={panorama_progress['errors']}"
                            )
                        pbar.update(1)

            elapsed_time = time.time() - start_time
            logging.info("Парсинг 360 завершён за %.1f сек (%.2f сек/спек) | "
                         "Цвета: +%d | Фото: +%d | Без 360: %d | Ошибок: %d",
                         elapsed_time,
                         elapsed_time / max(panorama_progress["parsed"], 1),
                         panorama_progress["colors_inserted"],
                         panorama_progress["panorama_photos_inserted"],
                         panorama_progress["skipped_no_panorama"],
                         panorama_progress["errors"])
            summary["panorama_colors"] += panorama_progress["colors_inserted"]
            summary["panorama_photos"] += panorama_progress["panorama_photos_inserted"]
    
    # Загрузка 360 фото (если включена)
    if settings.download_panoramas:
        logging.info("Начало загрузки 360-градусных фото в файловую систему")

        with session_scope(session_factory) as session:
            specs_with_panoramas = session.query(PanoramaPhoto.spec_id).distinct().all()
            spec_ids_for_download = [s[0] for s in specs_with_panoramas]

        if not spec_ids_for_download:
            logging.info("Нет комплектаций с 360 фото для загрузки.")
        else:
            img_path = Path(settings.img_path)
            img_path.mkdir(parents=True, exist_ok=True)

            logging.info("Найдено %d комплектаций для загрузки 360 фото → %s",
                         len(spec_ids_for_download), img_path.absolute())

            panorama_download_progress = {
                "lock": threading.Lock(),
                "total": len(spec_ids_for_download),
                "parsed": 0,
                "downloaded": 0,
                "skipped": 0,
                "download_errors": 0,
            }

            logging.info("Запуск загрузки 360 фото в %d потоках...", settings.parse_workers)
            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.parse_workers
            ) as executor:
                photo_workers_per_spec = min(5, settings.parse_workers // 2) if settings.parse_workers > 1 else 5

                futures = {
                    executor.submit(
                        _download_panoramas_for_spec,
                        spec_id,
                        session_factory,
                        str(img_path),
                        settings.api_timeout * 3,
                        panorama_download_progress,
                        photo_workers_per_spec,
                    ): spec_id
                    for spec_id in spec_ids_for_download
                }

                with _make_pbar(len(spec_ids_for_download), "Загрузка 360", "спек", colour="magenta") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        sid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            pass
                        with panorama_download_progress["lock"]:
                            elapsed = time.time() - start_time
                            dl = panorama_download_progress["downloaded"]
                            speed = dl / elapsed if elapsed > 0 else 0
                            pbar.set_description_str(
                                f"Загрузка 360 спек={sid} | "
                                f"↓={dl} проп={panorama_download_progress['skipped']} "
                                f"({speed:.1f} ф/с) "
                                f"ош={panorama_download_progress['download_errors']}"
                            )
                        pbar.update(1)

            elapsed_time = time.time() - start_time
            logging.info("Загрузка 360 завершена за %.1f сек (%.2f сек/спек) | "
                         "Скачано: %d, пропущено: %d | Ошибок: %d",
                         elapsed_time,
                         elapsed_time / max(panorama_download_progress["parsed"], 1),
                         panorama_download_progress["downloaded"],
                         panorama_download_progress["skipped"],
                         panorama_download_progress["download_errors"])
            summary["panorama_downloaded"] = panorama_download_progress["downloaded"]
            summary["panorama_download_skipped"] = panorama_download_progress["skipped"]
    
    # Итоговый отчёт
    total_downloaded = summary["photos_downloaded"] + summary["panorama_downloaded"]

    logging.info("=" * 60)
    logging.info("ИТОГОВЫЙ ОТЧЁТ")
    logging.info("Фото в БД: +%d обн %d | Скачано: %d (проп %d)",
                 summary["photos_added"], summary["photos_updated"],
                 summary["photos_downloaded"], summary["photos_download_skipped"])
    logging.info("360 в БД: +%d (цветов %d) | Скачано: %d (проп %d)",
                 summary["panorama_photos"], summary["panorama_colors"],
                 summary["panorama_downloaded"], summary["panorama_download_skipped"])
    logging.info("Всего скачано файлов: %d", total_downloaded)
    logging.info("=" * 60)

    # Красивый итоговый отчёт в терминал
    _print()
    _print("=" * 60)
    _print("  ИТОГОВЫЙ ОТЧЁТ")
    _print("=" * 60)
    _print(f"  Фото в БД:       +{summary['photos_added']}  обн {summary['photos_updated']}")
    _print(f"  Фото скачано:     {summary['photos_downloaded']}  (пропущено {summary['photos_download_skipped']})")
    _print(f"  360 фото в БД:    +{summary['panorama_photos']}  (цветов: {summary['panorama_colors']})")
    _print(f"  360 фото скачано: {summary['panorama_downloaded']}  (пропущено {summary['panorama_download_skipped']})")
    _print(f"  ─────────────────────────────")
    _print(f"  Всего скачано:    {total_downloaded} файлов")
    _print("=" * 60)


if __name__ == "__main__":
    main()
