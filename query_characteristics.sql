-- ============================================
-- Характеристики для серии с ID 3895
-- ============================================

-- Вариант 1: Все характеристики с группировкой по категориям
SELECT 
    s.name AS "Серия",
    b.name AS "Марка",
    sp.name AS "Комплектация",
    pt.group_name AS "Категория",
    pv.item_name AS "Характеристика",
    CASE 
        WHEN pv.sub_name IS NOT NULL AND pv.sub_name != '' 
        THEN pv.sub_name 
        ELSE NULL 
    END AS "Подхарактеристика",
    pv.value AS "Значение"
FROM series s
JOIN brands b ON b.id = s.brand_id
JOIN specs sp ON sp.series_id = s.id
JOIN param_values pv ON pv.spec_id = sp.id
LEFT JOIN param_titles pt ON pt.title_id = pv.title_id
WHERE s.id = 3895
ORDER BY sp.id, pt.sort NULLS LAST, pt.item_id, pv.sub_name;

-- ============================================
-- Вариант 2: Только основные характеристики (без подхарактеристик)
-- ============================================
-- SELECT 
--     s.name AS "Серия",
--     b.name AS "Марка",
--     sp.name AS "Комплектация",
--     pt.group_name AS "Категория",
--     pv.item_name AS "Характеристика",
--     pv.value AS "Значение"
-- FROM series s
-- JOIN brands b ON b.id = s.brand_id
-- JOIN specs sp ON sp.series_id = s.id
-- JOIN param_values pv ON pv.spec_id = sp.id
-- LEFT JOIN param_titles pt ON pt.title_id = pv.title_id
-- WHERE s.id = 3895
--   AND (pv.sub_name IS NULL OR pv.sub_name = '')
-- ORDER BY sp.id, pt.sort NULLS LAST, pt.item_id;

-- ============================================
-- Вариант 3: Сводная таблица (характеристики в колонках)
-- ============================================
-- SELECT 
--     sp.name AS "Комплектация",
--     MAX(CASE WHEN pv.item_name = '厂商指导价(元)' THEN pv.value END) AS "Цена",
--     MAX(CASE WHEN pv.item_name = '整备质量(kg)' THEN pv.value END) AS "Масса (кг)",
--     MAX(CASE WHEN pv.item_name = '官方0-100km/h加速(s)' THEN pv.value END) AS "Разгон 0-100 (с)",
--     MAX(CASE WHEN pv.item_name = '最大功率(kW)' THEN pv.value END) AS "Мощность (кВт)",
--     MAX(CASE WHEN pv.item_name = '最大扭矩(N·m)' THEN pv.value END) AS "Крутящий момент (Н·м)"
-- FROM series s
-- JOIN specs sp ON sp.series_id = s.id
-- JOIN param_values pv ON pv.spec_id = sp.id
-- WHERE s.id = 3895
--   AND (pv.sub_name IS NULL OR pv.sub_name = '')
-- GROUP BY sp.id, sp.name
-- ORDER BY sp.id;

-- ============================================
-- Вариант 4: Проверка конкретных характеристик (для отладки)
-- ============================================
-- SELECT 
--     sp.name AS "Комплектация",
--     pv.title_id AS "Title ID",
--     pv.item_name AS "Название",
--     pv.value AS "Значение",
--     pv.sub_name AS "Подназвание"
-- FROM series s
-- JOIN specs sp ON sp.series_id = s.id
-- JOIN param_values pv ON pv.spec_id = sp.id
-- WHERE s.id = 3895
--   AND pv.title_id IN (3, 15, 19)  -- Цена, Разгон, Масса
-- ORDER BY sp.id, pv.title_id;
