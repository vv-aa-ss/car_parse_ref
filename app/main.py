from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from typing import Iterable, List

from app.config import load_settings
from app.db import create_db_engine, create_session_factory, ensure_database_exists, session_scope
from app.models import Base, Spec
from app.parser.autohome_client import AutohomeClient
from app.parser.parsers import limit_series_per_brand, parse_param_conf, parse_tree_menu
from app.repository import (
    upsert_brands,
    upsert_factories,
    upsert_param_titles,
    upsert_param_values,
    upsert_series,
    upsert_specs,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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
    series_id: int, timeout: float, session_factory, progress_counter: dict
) -> None:
    max_retries = 3
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            with AutohomeClient(timeout=timeout) as client:
                payload = client.get_param_conf(series_id)
            parsed = parse_param_conf(payload, series_id)
            
            with session_scope(session_factory) as session:
                titles_stats = upsert_param_titles(session, parsed["titles"])
                specs_stats = upsert_specs(session, parsed["specs"])
                values_stats = upsert_param_values(session, parsed["param_values"])
            
            # Успешно обработано - обновляем счетчики
            with progress_counter["lock"]:
                progress_counter["parsed"] += 1
                progress_counter["specs_inserted"] += specs_stats["inserted"]
                progress_counter["specs_updated"] += specs_stats["updated"]
                progress_counter["specs_skipped"] += specs_stats["skipped"]
                progress_counter["values_inserted"] += values_stats["inserted"]
                progress_counter["values_updated"] += values_stats["updated"]
                
                total = progress_counter["total"]
                parsed = progress_counter["parsed"]
                errors = progress_counter["errors"]
                
                # Логируем каждые 5 серий для лучшей видимости работы потоков
                if parsed % 5 == 0 or parsed == total:
                    percentage = (parsed / total * 100) if total > 0 else 0
                    logging.info(
                        "Прогресс: %d/%d (%.1f%%) | "
                        "Спецификации: +%d обновлено %d | "
                        "Параметры: +%d обновлено %d | "
                        "Ошибок: %d",
                        parsed,
                        total,
                        percentage,
                        progress_counter["specs_inserted"],
                        progress_counter["specs_updated"],
                        progress_counter["values_inserted"],
                        progress_counter["values_updated"],
                        errors,
                    )
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


def main() -> None:
    settings = load_settings()
    
    logging.info("=" * 60)
    logging.info("Запуск парсера автомобилей Autohome")
    logging.info("=" * 60)
    logging.info("Настройки: MODELS_PER_BRAND=%d, PARSE_WORKERS=%d", settings.models_per_brand, settings.parse_workers)
    
    logging.info("Загрузка дерева брендов и моделей...")
    with AutohomeClient(timeout=settings.api_timeout) as client:
        tree_payload = client.get_tree_menu()

    parsed_tree = parse_tree_menu(tree_payload)
    
    logging.info(
        "Получено из API: %d брендов, %d фабрик, %d серий",
        len(parsed_tree["brands"]),
        len(parsed_tree["factories"]),
        len(parsed_tree["series"]),
    )
    
    # Применяем лимит количества брендов
    if settings.models_per_brand > 0:
        logging.info("Применяется лимит: %d брендов (со всеми их моделями)", settings.models_per_brand)
        series_limited = limit_series_per_brand(
            parsed_tree["series"], settings.models_per_brand
        )
        
        # Подсчитываем сколько брендов попало в выборку
        limited_brand_ids = set(s.brand_id for s in series_limited)
        logging.info(
            "После применения лимита: %d брендов, %d серий (было %d брендов, %d серий)",
            len(limited_brand_ids),
            len(series_limited),
            len(parsed_tree["brands"]),
            len(parsed_tree["series"]),
        )
    else:
        series_limited = parsed_tree["series"]
        logging.info("Лимит брендов не применен (MODELS_PER_BRAND=0 или не указан)")
    
    series_ids = [series.id for series in series_limited]

    if not settings.use_database:
        logging.info("Режим без БД: данные не сохраняются")
        return

    logging.info("Проверка и создание базы данных...")
    ensure_database_exists(settings)
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(engine)

    logging.info("Сохранение брендов, фабрик и серий в БД...")
    with session_scope(session_factory) as session:
        brands_stats = upsert_brands(session, parsed_tree["brands"])
        logging.info(
            "Бренды: добавлено %d, обновлено %d, пропущено %d",
            brands_stats["inserted"],
            brands_stats["updated"],
            brands_stats["skipped"],
        )
        
        factories_stats = upsert_factories(session, parsed_tree["factories"])
        logging.info(
            "Фабрики: добавлено %d, обновлено %d, пропущено %d",
            factories_stats["inserted"],
            factories_stats["updated"],
            factories_stats["skipped"],
        )
        
        series_stats = upsert_series(session, series_limited)
        logging.info(
            "Серии: добавлено %d, обновлено %d, пропущено %d",
            series_stats["inserted"],
            series_stats["updated"],
            series_stats["skipped"],
        )

    logging.info("Фильтрация серий для парсинга...")
    series_ids = _filter_series_for_reparse(
        session_factory, series_ids, settings.force_reparse
    )

    if not series_ids:
        logging.info("Нет новых серий для парсинга.")
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
            ): series_id
            for series_id in series_ids
        }
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            try:
                future.result()
            except Exception:
                pass  # Ошибка уже залогирована в _parse_and_store_series
            
            # Логируем каждые 50 завершенных задач для видимости работы потоков
            if completed % 50 == 0:
                with progress_counter["lock"]:
                    parsed = progress_counter["parsed"]
                    errors = progress_counter["errors"]
                logging.info(
                    "Активных задач: %d/%d | Обработано: %d | Ошибок: %d",
                    len(futures) - completed,
                    len(futures),
                    parsed,
                    errors,
                )

    elapsed_time = time.time() - start_time
    logging.info("-" * 60)
    logging.info("Парсинг завершен за %.1f секунд!", elapsed_time)
    logging.info(
        "Итого: обработано %d серий, ошибок %d",
        progress_counter["parsed"],
        progress_counter["errors"],
    )
    logging.info(
        "Спецификации: добавлено %d, обновлено %d, пропущено %d",
        progress_counter["specs_inserted"],
        progress_counter["specs_updated"],
        progress_counter["specs_skipped"],
    )
    logging.info(
        "Параметры: добавлено %d, обновлено %d",
        progress_counter["values_inserted"],
        progress_counter["values_updated"],
    )
    if progress_counter["parsed"] > 0:
        avg_time = elapsed_time / progress_counter["parsed"]
        logging.info("Средняя скорость: %.2f сек/серия", avg_time)
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
