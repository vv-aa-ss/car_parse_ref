"""
Модуль перевода текстов с кэшированием.

Использует deep-translator (Google Translate, бесплатно, без API ключа).
Кэширует переводы в таблице translation_cache, чтобы не запрашивать повторно.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Optional

from deep_translator import GoogleTranslator
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.brand_dict import BRAND_NAMES
from app.db import session_scope
from app.models import TranslationCache

logger = logging.getLogger(__name__)

# Регулярное выражение для поиска китайских символов
_CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")

# Лимиты deep-translator (Google free endpoint)
_MAX_BATCH_SIZE = 50  # Макс. текстов за один вызов translate_batch
_MAX_CHARS_PER_TEXT = 4900  # Лимит ~5000 символов на один текст


def _sha256(text: str) -> str:
    """SHA256 хеш строки."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def has_chinese(text: str) -> bool:
    """Проверяет, содержит ли текст китайские символы."""
    if not text:
        return False
    return bool(_CHINESE_RE.search(text))


def needs_translation(text: Optional[str]) -> bool:
    """
    Определяет, нужно ли переводить текст.
    Пропускает: None, пустые строки, чистые числа, символы (●, ○, -, /).
    """
    if not text or not text.strip():
        return False
    stripped = text.strip()
    # Чистые числа и единицы (4998, 252, 1.5T, 4WD и т.д.)
    if re.match(r"^[\d.,×xX\-+/%()\s·°LTkWNmrpmhkgmmcm³²PSiVAHCcGPabar]+$", stripped):
        return False
    # Символы-маркеры
    if stripped in ("●", "○", "-", "—", "/", "★", "☆", "■", "□"):
        return False
    return has_chinese(stripped)


class Translator:
    """
    Переводчик с кэшированием в БД.
    Использует Google Translate через deep-translator (бесплатно, без API ключа).
    
    Использование:
        translator = Translator(session_factory=...)
        results = translator.translate_batch(["你好", "世界"], target_lang="ru")
        # results = {"你好": "Привет", "世界": "Мир"}
    """

    def __init__(self, session_factory, source_lang: str = "zh-CN"):
        self._session_factory = session_factory
        self._source_lang = source_lang
        # Счётчики
        self.stats = {"api_calls": 0, "api_chars": 0, "cache_hits": 0, "translated": 0}

    def _lookup_cache(self, texts: list[str], target_lang: str) -> dict[str, str]:
        """Ищет переводы в кэше БД. Возвращает {source_text: translated_text}."""
        if not texts:
            return {}
        
        hashes = [_sha256(t) for t in texts]
        result = {}
        
        with session_scope(self._session_factory) as session:
            rows = (
                session.query(
                    TranslationCache.source_hash,
                    TranslationCache.source_text,
                    TranslationCache.translated_text,
                )
                .filter(
                    TranslationCache.target_lang == target_lang,
                    TranslationCache.source_hash.in_(hashes),
                )
                .all()
            )
            for row in rows:
                result[row.source_text] = row.translated_text

        return result

    def _save_cache(self, translations: dict[str, str], target_lang: str) -> None:
        """Сохраняет переводы в кэш БД."""
        if not translations:
            return
        
        records = []
        for source_text, translated_text in translations.items():
            records.append({
                "source_hash": _sha256(source_text),
                "source_text": source_text,
                "target_lang": target_lang,
                "translated_text": translated_text,
            })
        
        with session_scope(self._session_factory) as session:
            stmt = pg_insert(TranslationCache).values(records)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_translation_hash_lang",
                set_={"translated_text": stmt.excluded.translated_text},
            )
            session.execute(stmt)

    def _lookup_brand_dict(self, text: str) -> str | None:
        """
        Проверяет текст в словаре брендов.
        
        Работает в двух режимах:
        1. Точное совпадение: "大众" → "Volkswagen"
        2. Замена бренда в составном тексте: "大众朗逸" → подставляет "Volkswagen"
           в начало, а остаток переводит отдельно
        """
        # Точное совпадение
        if text in BRAND_NAMES:
            return BRAND_NAMES[text]
        return None

    def _replace_brands_in_text(self, text: str) -> str:
        """
        Заменяет китайские названия брендов на английские в составном тексте.
        Сортировка по длине ключа (от длинных к коротким) для корректной замены.
        """
        result = text
        # Сортируем по длине (длинные первые), чтобы "长安欧尚" заменился раньше "长安"
        for cn_name, en_name in sorted(BRAND_NAMES.items(), key=lambda x: len(x[0]), reverse=True):
            if cn_name in result:
                result = result.replace(cn_name, en_name)
        return result

    def _call_translate_api(self, texts: list[str], target_lang: str) -> list[str]:
        """
        Переводит тексты через Google Translate (deep-translator).
        
        Поддерживает retry при ошибках и rate-limit.
        """
        max_retries = 5
        translator = GoogleTranslator(source=self._source_lang, target=target_lang)
        
        results = []
        for text in texts:
            # Обрезаем слишком длинные тексты
            if len(text) > _MAX_CHARS_PER_TEXT:
                text = text[:_MAX_CHARS_PER_TEXT]
            
            translated = None
            for attempt in range(max_retries):
                try:
                    translated = translator.translate(text)
                    break
                except Exception as e:
                    err_str = str(e).lower()
                    if "too many requests" in err_str or "429" in err_str:
                        wait = (attempt + 1) * 3
                        logger.warning(f"Rate limit, ждём {wait} сек...")
                        time.sleep(wait)
                    elif attempt < max_retries - 1:
                        wait = (attempt + 1) * 2
                        logger.warning(
                            f"Ошибка перевода (попытка {attempt+1}/{max_retries}): {e}, "
                            f"ждём {wait} сек..."
                        )
                        time.sleep(wait)
                    else:
                        logger.error(f"Не удалось перевести '{text[:50]}...': {e}")
                        translated = ""
            
            results.append(translated or "")
            self.stats["api_chars"] += len(text)
        
        self.stats["api_calls"] += len(texts)
        
        # Пауза между батчами для избежания rate-limit
        if len(texts) > 10:
            time.sleep(1.0)
        elif len(texts) > 0:
            time.sleep(0.3)
        
        return results

    def translate_batch(
        self,
        texts: list[str],
        target_lang: str,
        skip_non_chinese: bool = True,
    ) -> dict[str, str]:
        """
        Переводит батч текстов с кэшированием.
        
        Args:
            texts: Список строк для перевода
            target_lang: Целевой язык ('en' или 'ru')
            skip_non_chinese: Пропускать строки без китайских символов
        
        Returns:
            Словарь {исходный_текст: перевод}
        """
        if not texts:
            return {}
        
        # Убираем дубликаты, сохраняя порядок
        unique_texts = list(dict.fromkeys(t for t in texts if t and t.strip()))
        
        # Фильтруем тексты, не нуждающиеся в переводе
        if skip_non_chinese:
            to_translate = [t for t in unique_texts if needs_translation(t)]
            no_translate = {t: t for t in unique_texts if not needs_translation(t)}
        else:
            to_translate = unique_texts
            no_translate = {}
        
        if not to_translate:
            return no_translate
        
        # Сначала проверяем словарь брендов (только для EN)
        from_dict = {}
        remaining = []
        for t in to_translate:
            dict_result = self._lookup_brand_dict(t)
            if dict_result:
                from_dict[t] = dict_result
            else:
                remaining.append(t)
        
        if from_dict:
            self.stats["cache_hits"] += len(from_dict)
        
        # Ищем в кэше БД (только то, чего нет в словаре)
        cached = self._lookup_cache(remaining, target_lang) if remaining else {}
        self.stats["cache_hits"] += len(cached)
        
        # Определяем, что ещё не переведено
        uncached = [t for t in remaining if t not in cached]
        
        # Для EN: заменяем бренды в составных текстах перед отправкой в API
        if target_lang == "en" and uncached:
            preprocessed = []
            for text in uncached:
                replaced = self._replace_brands_in_text(text)
                preprocessed.append(replaced)
        else:
            preprocessed = uncached
        
        # Переводим через API
        new_translations = {}
        if uncached:
            # Разбиваем на батчи
            for i in range(0, len(uncached), _MAX_BATCH_SIZE):
                batch_original = uncached[i:i + _MAX_BATCH_SIZE]
                batch_to_send = preprocessed[i:i + _MAX_BATCH_SIZE]
                translated_list = self._call_translate_api(batch_to_send, target_lang)
                
                for source, translated_text in zip(batch_original, translated_list):
                    if translated_text:
                        new_translations[source] = translated_text
                        self.stats["translated"] += 1
            
            # Сохраняем новые переводы в кэш
            self._save_cache(new_translations, target_lang)
        
        # Объединяем результаты
        result = {}
        result.update(no_translate)
        result.update(from_dict)
        result.update(cached)
        result.update(new_translations)
        
        return result
