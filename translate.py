"""
Скрипт перевода данных в БД с китайского на русский/английский.

Использование:
    python translate.py

Требует GOOGLE_TRANSLATE_API_KEY в .env
Работает с базой DB_NAME из .env (рекомендуется cars_test для тестирования).
"""
from __future__ import annotations

import logging
import os
import time

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

from app.config import load_settings
from app.db import create_db_engine, create_session_factory, session_scope
from app.models import (
    Base,
    Brand,
    Series,
    Spec,
    ParamTitle,
    ParamValue,
    PhotoColor,
    PhotoCategory,
    PanoramaColor,
    TranslationCache,
)
from app.translator import Translator, needs_translation

logger = logging.getLogger(__name__)

# Описание задач перевода: (Модель, исходное_поле, поле_перевода, язык)
TRANSLATION_TASKS = [
    # На английский: бренды, серии, комплектации
    {"model": Brand, "source": "name", "target": "name_en", "lang": "en", "label": "Бренды → EN"},
    {"model": Series, "source": "name", "target": "name_en", "lang": "en", "label": "Серии → EN"},
    {"model": Spec, "source": "name", "target": "name_en", "lang": "en", "label": "Комплектации → EN"},
    # На русский: параметры, значения, цвета, категории
    {"model": ParamTitle, "source": "item_name", "target": "item_name_ru", "lang": "ru", "label": "Параметры (item_name) → RU"},
    {"model": ParamTitle, "source": "group_name", "target": "group_name_ru", "lang": "ru", "label": "Параметры (group_name) → RU"},
    {"model": ParamValue, "source": "value", "target": "value_ru", "lang": "ru", "label": "Значения → RU"},
    {"model": PhotoColor, "source": "name", "target": "name_ru", "lang": "ru", "label": "Цвета фото → RU"},
    {"model": PhotoCategory, "source": "name", "target": "name_ru", "lang": "ru", "label": "Категории фото → RU"},
    {"model": PanoramaColor, "source": "color_name", "target": "color_name_ru", "lang": "ru", "label": "Цвета 360 → RU"},
    {"model": PanoramaColor, "source": "base_color_name", "target": "base_color_name_ru", "lang": "ru", "label": "Базовые цвета 360 → RU"},
]


def _ensure_columns_exist(engine) -> None:
    """Создаёт недостающие таблицы и колонки в БД."""
    from sqlalchemy import inspect, text as sa_text

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    # Создаём таблицу translation_cache если её нет
    if "translation_cache" not in existing_tables:
        TranslationCache.__table__.create(engine)
        logger.info("Создана таблица translation_cache")

    # Добавляем недостающие колонки к существующим таблицам
    new_columns = [
        ("brands", "name_en", "VARCHAR(255)"),
        ("series", "name_en", "VARCHAR(255)"),
        ("specification", "name_en", "VARCHAR(512)"),
        ("param_titles", "item_name_ru", "VARCHAR(512)"),
        ("param_titles", "group_name_ru", "VARCHAR(255)"),
        ("param_values", "value_ru", "TEXT"),
        ("photo_colors", "name_ru", "VARCHAR(255)"),
        ("photo_categories", "name_ru", "VARCHAR(255)"),
        ("panorama_colors", "color_name_ru", "VARCHAR(255)"),
        ("panorama_colors", "base_color_name_ru", "VARCHAR(255)"),
    ]

    with engine.connect() as conn:
        for table_name, col_name, col_type in new_columns:
            if table_name not in existing_tables:
                continue
            existing_cols = [c["name"] for c in inspector.get_columns(table_name)]
            if col_name not in existing_cols:
                conn.execute(sa_text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type}'))
                logger.info(f"Добавлена колонка {table_name}.{col_name}")
        conn.commit()


def _translate_task(
    task: dict,
    translator: Translator,
    session_factory,
    batch_size: int = 500,
    force: bool = False,
) -> dict:
    """
    Выполняет одну задачу перевода в два этапа:
    1) Собираем уникальные тексты → переводим (с прогрессом)
    2) Записываем переводы в БД (с прогрессом)
    
    Args:
        task: Описание задачи (model, source, target, lang, label)
        translator: Экземпляр Translator
        session_factory: Фабрика сессий
        batch_size: Размер батча для обработки записей из БД
        force: Принудительно перевести даже если уже есть перевод
    
    Returns:
        Статистика: {"total": ..., "translated": ..., "skipped": ..., "cached": ...}
    """
    model = task["model"]
    source_field = task["source"]
    target_field = task["target"]
    target_lang = task["lang"]
    label = task["label"]
    source_col = model.__table__.c[source_field]
    target_col = model.__table__.c[target_field]

    stats = {"total": 0, "translated": 0, "skipped": 0, "cached": 0}

    # ── Этап 1: собираем уникальные тексты, требующие перевода ──
    from sqlalchemy import func, distinct

    with session_scope(session_factory) as session:
        total_count = session.query(model).count()
        if total_count == 0:
            return stats
        stats["total"] = total_count

        # Уникальные исходные тексты, у которых ещё нет перевода
        query = session.query(distinct(source_col)).filter(source_col.isnot(None), source_col != "")
        if not force:
            query = query.filter((target_col.is_(None)) | (target_col == ""))
        
        unique_texts_rows = query.all()
        unique_texts = [row[0] for row in unique_texts_rows if row[0]]

    if not unique_texts:
        # Всё уже переведено или нечего переводить
        stats["skipped"] = total_count
        print(f"  Все {total_count} записей уже переведены, пропускаем")
        return stats

    # Фильтруем: только строки с китайскими символами
    chinese_texts = [t for t in unique_texts if needs_translation(t)]
    non_chinese_texts = [t for t in unique_texts if not needs_translation(t)]

    print(f"  Всего записей: {total_count}, уникальных текстов для перевода: {len(chinese_texts)}, без перевода (лат/цифры): {len(non_chinese_texts)}")

    # ── Этап 2: переводим уникальные тексты (с прогрессом) ──
    translation_map = {}

    # Не-китайские тексты оставляем как есть
    for t in non_chinese_texts:
        translation_map[t] = t.strip()

    if chinese_texts:
        # Проверяем кэш
        cached = translator._lookup_cache(chinese_texts, target_lang)
        translator.stats["cache_hits"] += len(cached)
        translation_map.update(cached)

        uncached = [t for t in chinese_texts if t not in cached]
        
        if cached:
            print(f"  Из кэша: {len(cached)}")
        
        if uncached:
            print(f"  Нужно перевести: {len(uncached)}")
            pbar = tqdm(total=len(uncached), desc=f"{label} [API]", unit="текст", leave=True)
            
            new_translations = {}
            for text in uncached:
                translated_list = translator._call_translate_api([text], target_lang)
                translated = translated_list[0] if translated_list else ""
                if translated:
                    new_translations[text] = translated
                    translation_map[text] = translated
                pbar.update(1)
            
            pbar.close()

            # Сохраняем в кэш
            if new_translations:
                translator._save_cache(new_translations, target_lang)
                translator.stats["translated"] += len(new_translations)

    # ── Этап 3: записываем переводы в БД батчами ──
    if translation_map:
        pbar_db = tqdm(total=total_count, desc=f"{label} [БД]", unit="зап", leave=True)
        offset = 0

        while offset < total_count:
            with session_scope(session_factory) as session:
                rows = session.query(model).offset(offset).limit(batch_size).all()
                if not rows:
                    break

                for row in rows:
                    source_text = getattr(row, source_field)
                    existing = getattr(row, target_field)

                    if existing and not force:
                        stats["skipped"] += 1
                    elif source_text and source_text in translation_map:
                        setattr(row, target_field, translation_map[source_text])
                        stats["translated"] += 1
                    else:
                        stats["skipped"] += 1
                    
                    pbar_db.update(1)

            offset += batch_size

        pbar_db.close()

    return stats


def main():
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Загружаем настройки
    settings = load_settings()
    force = os.getenv("FORCE_TRANSLATE", "false").lower() in ("true", "1", "yes")

    print(f"\n{'='*60}")
    print(f"  Перевод данных: {settings.db_name}")
    print(f"  Режим: {'принудительный (FORCE)' if force else 'только новые'}")
    print(f"{'='*60}\n")

    # Подключаемся к БД
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)

    # Создаём недостающие таблицы/колонки
    _ensure_columns_exist(engine)

    # Создаём переводчик (Google Translate, бесплатно, без API ключа)
    translator = Translator(
        session_factory=session_factory,
        source_lang="zh-CN",
    )

    # Выполняем задачи перевода
    total_stats = {"total": 0, "translated": 0, "skipped": 0}
    start_time = time.time()

    for task in TRANSLATION_TASKS:
        print(f"\n--- {task['label']} ---")
        task_stats = _translate_task(
            task=task,
            translator=translator,
            session_factory=session_factory,
            force=force,
        )
        total_stats["total"] += task_stats["total"]
        total_stats["translated"] += task_stats["translated"]
        total_stats["skipped"] += task_stats["skipped"]
        
        print(f"  Записей: {task_stats['total']}, переведено: {task_stats['translated']}, пропущено: {task_stats['skipped']}")

    elapsed = time.time() - start_time

    # Итоговый отчёт
    print(f"\n{'='*60}")
    print(f"  ИТОГО")
    print(f"  Записей обработано: {total_stats['total']}")
    print(f"  Переведено: {total_stats['translated']}")
    print(f"  Пропущено: {total_stats['skipped']}")
    print(f"  API-запросов: {translator.stats['api_calls']}")
    print(f"  Символов отправлено: {translator.stats['api_chars']:,}")
    print(f"  Из кэша: {translator.stats['cache_hits']}")
    print(f"  Время: {elapsed:.1f} сек")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
