1. brands — Бренды автомобилей
id(PK, int)	- ID бренда из API
name(string) -	Название (BMW, Audi, 奔驰…)
logo_url(text) - URL логотипа

2. series — Модельные ряды (серии)
id(PK, int) - ID серии из API
brand_id(FK → brands) - К какому бренду относится
name(string) -	Название (X5, A6L, 宝马3系…)
is_new_energy(bool) - Электро/гибрид?

3. specification — Комплектации (конкретные модели)
id(PK, int) -	ID комплектации из API
series_id(FK → series) -	К какой серии относится
name(string) -	Полное название («2024款 530Li 豪华套装»)
min_price(string) -	Минимальная цена

4. param_titles — Справочник параметров (заголовки характеристик)
series_id(PK, FK → series) -	К какой серии(Привязывает параметр к конкретной серии авто. Один и тот же title_id от API может встречаться в разных сериях — именно series_id разделяет их.)
title_id(PK, int) -	ID параметра из API (например, «Объём двигателя», «Мощность» и т.д.). Именно по title_id связываются param_titles и param_values
item_name(string) -	Название параметра («Длина», «Мощность», «Тип КПП»…)
group_name(string) -	Группа («Двигатель», «Кузов», «Безопасность»…)
item_type(string) -	Тип параметра

5. param_values — Значения характеристик для каждой комплектации
specification_id(PK, FK → specification) -	Комплектация(spec_id) (конкретной модели)
title_id(PK, int) -	ID параметра (связь с param_titles)
item_name(PK, string) -	Название параметра
sub_name(PK, string) -	Подпараметр (если есть)
value(text) - Значение («4998mm», «252л.с.», «●»…)

6. photo_colors — Цвета для обычных фото
id(PK, int) -	ID цвета из API
series_id(FK → series) -	К какой серии
color_type(string) -	"exterior" (кузов) или "interior" (салон)
name(string) -	Название («黑色(Черный)», «白色(Белый)», «银石蓝»…)
value(string) -	HEX код (#000000, #FFFFFF…)
isonsale(bool) -	В продаже ли этот цвет

7. photo_categories — Категории фото
id(PK, int) -	ID категории (1=外观(Внешний вид), 3=中控, 10=细节(Детали), 12=空间…)
series_id(PK, FK → series) -	К какой серии
name(string) -	Название категории(экстерьер автомобиля)

8. photos — Обычные фотографии
id(PK, string) - ID фото из API(без него невозможно отличить одно фото от другого, нужен для обновления данных)
series_id(FK → series) - Серия
specification_id(FK → specification) - Комплектация
category_id(int) -	Категория (→ photo_categories)
color_id(int) -	Цвет (→ photo_colors, 0 = не указан)
originalpic(text) -	URL оригинала
specname(string) -	Название комплектации
local_path(text) -	Локальный путь после скачивания

9. panorama_colors — Цвета для 360° фото
id(PK, int) - ID записи из API(Используется как PK для upsert — чтобы при повторном парсинге обновить, а не дублировать.)
spec_id(FK → specification) - Комплектация (привязывает цвет к конкретной комплектации)
ext_id(int) - ID панорамы (По сути кэш для ускорения повторного парсинга)
base_color_name(string) - Базовое название цвета
color_name(string) - Полное название цвета
color_value(string) - HEX код
color_id(int) -	ColorId (используется в запросах API)

10. panorama_photos — 360° фотографии (кадры панорамы)
id(PK, string) - Составной ID (seq+spec_id+color_id, PK для upsert и поиска в БД при скачивании)
spec_id(FK → specification) - Привязка к комплектации
color_id(int) -	ColorId (→ panorama_colors)
seq(int) -	Порядковый номер кадра (0, 1, 2… до 71)
url(text) -	URL кадра
local_path(text) -	Локальный путь после скачивания