from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class BrandData:
    id: int
    name: str
    logo_url: Optional[str]


@dataclass(frozen=True)
class SeriesData:
    id: int
    brand_id: int
    name: str
    is_new_energy: Optional[bool]


@dataclass(frozen=True)
class SpecData:
    id: int
    series_id: int
    name: str
    min_price: Optional[str]


@dataclass(frozen=True)
class ParamTitleData:
    series_id: int
    title_id: int
    item_name: str
    group_name: Optional[str]
    item_type: Optional[str]


@dataclass(frozen=True)
class ParamValueData:
    specification_id: int
    title_id: int
    item_name: str
    sub_name: Optional[str]
    value: Optional[str]


@dataclass(frozen=True)
class PhotoColorData:
    id: int
    series_id: int
    color_type: str  # "interior" или "exterior"
    name: str
    value: Optional[str]

    isonsale: Optional[bool]


@dataclass(frozen=True)
class PhotoCategoryData:
    id: int
    series_id: int
    name: str


@dataclass(frozen=True)
class PhotoData:
    id: str  # ID фото из API
    series_id: int
    specification_id: int
    category_id: int
    color_id: int
    originalpic: Optional[str]

    specname: Optional[str]


@dataclass(frozen=True)
class PanoramaColorData:
    """Данные о цвете для 360-градусных фото."""
    id: int  # Id из color_info
    spec_id: int
    ext_id: Optional[int]  # ext.Id из baseinfo
    base_color_name: Optional[str]  # BaseColorName
    color_name: str  # ColorName
    color_value: Optional[str]  # ColorValue (HEX)
    color_id: int  # ColorId (используется в getVrInfo)


@dataclass(frozen=True)
class PanoramaPhotoData:
    """Данные о 360-градусном фото."""
    id: str  # Уникальный ID (spec_id_color_id_seq)
    spec_id: int
    color_id: int  # ColorId из PanoramaColor
    seq: int  # Порядковый номер кадра
    url: str  # URL фото


def parse_tree_menu(payload: Dict[str, Any]) -> Dict[str, List[Any]]:
    result = payload.get("result") or []
    brands: List[BrandData] = []
    series_list: List[SeriesData] = []

    for letter_group in result:
        for brand in letter_group.get("branditems") or []:
            brand_id = brand.get("id")
            if brand_id is None:
                continue
            brands.append(
                BrandData(
                    id=int(brand_id),
                    name=str(brand.get("name") or ""),
                    logo_url=brand.get("logo"),
                )
            )
            for factory in brand.get("fctitems") or []:
                for series in factory.get("seriesitems") or []:
                    series_id = series.get("id")
                    if series_id is None:
                        continue
                    series_list.append(
                        SeriesData(
                            id=int(series_id),
                            brand_id=int(brand_id),
                            name=str(series.get("name") or ""),
                            is_new_energy=bool(series.get("isnewenergy"))
                            if series.get("isnewenergy") is not None
                            else None,
                        )
                    )

    return {"brands": brands, "series": series_list}


def parse_param_conf(payload: Dict[str, Any], series_id: int) -> Dict[str, List[Any]]:
    result = payload.get("result") or {}
    titlelist = result.get("titlelist") or []
    datalist = result.get("datalist") or []

    titles: List[ParamTitleData] = []
    seen_title_ids: set[int] = set()
    for group in titlelist:
        group_name = group.get("groupname")
        item_type = group.get("itemtype")
        for item in group.get("items") or []:
            title_id = item.get("titleid")
            if title_id is None:
                continue
            tid = int(title_id)
            if tid in seen_title_ids:
                continue  # title_id уникален в рамках серии
            seen_title_ids.add(tid)
            titles.append(
                ParamTitleData(
                    series_id=series_id,
                    title_id=tid,
                    item_name=str(item.get("itemname") or ""),
                    group_name=group_name,
                    item_type=item_type,
                )
            )

    specs: List[SpecData] = []
    param_values: List[ParamValueData] = []
    for spec_item in datalist:
        spec_id = spec_item.get("specid")
        if spec_id is None:
            continue
        spec_id_int = int(spec_id)
        specs.append(
            SpecData(
                id=spec_id_int,
                series_id=series_id,
                name=str(spec_item.get("specname") or ""),
                min_price=spec_item.get("minprice"),
            )
        )

        for conf in spec_item.get("paramconflist") or []:
            title_id = conf.get("titleid")
            if title_id is None:
                continue
            
            # Находим название характеристики из titlelist по title_id
            # Это правильное название (например, "环保标准", "最大功率(kW)")
            matching_title = next(
                (t for t in titles if t.title_id == int(title_id)),
                None
            )
            
            if not matching_title:
                continue  # Пропускаем, если не нашли название в titlelist
            
            item_name = matching_title.item_name
            sublist = conf.get("sublist")
            
            if sublist:
                # Есть подсписок - обрабатываем каждый элемент
                for sub in sublist:
                    param_values.append(
                        ParamValueData(
                            specification_id=spec_id_int,
                            title_id=int(title_id),
                            item_name=item_name,
                            sub_name=sub.get("name") or "",
                            value=sub.get("value"),
                        )
                    )
            else:
                # Нет подсписка - значение находится в conf.itemname
                # В этом случае itemname из conf - это значение характеристики
                value = conf.get("itemname")
                
                param_values.append(
                    ParamValueData(
                        specification_id=spec_id_int,
                        title_id=int(title_id),
                        item_name=item_name,  # Название из titlelist
                        sub_name="",  # Пустая строка вместо None
                        value=value,  # Значение из conf.itemname
                    )
                )

    return {"titles": titles, "specs": specs, "param_values": param_values}


def limit_series_per_brand(
    series_list: Iterable[SeriesData], limit: int
) -> List[SeriesData]:
    """
    Ограничивает количество брендов (не моделей внутри бренда).
    Если limit=2, то берет только первые 2 бренда и все их модели.
    """
    if limit <= 0:
        return list(series_list)
    
    # Собираем все серии и группируем по brand_id
    series_by_brand: Dict[int, List[SeriesData]] = {}
    for series in series_list:
        if series.brand_id not in series_by_brand:
            series_by_brand[series.brand_id] = []
        series_by_brand[series.brand_id].append(series)
    
    # Берем только первые N брендов (по порядку появления)
    limited_brand_ids = list(series_by_brand.keys())[:limit]
    
    # Собираем все серии из выбранных брендов
    limited: List[SeriesData] = []
    for brand_id in limited_brand_ids:
        limited.extend(series_by_brand[brand_id])
    
    return limited


def parse_photo_info(payload: Dict[str, Any], series_id: int) -> Dict[str, List[Any]]:
    """
    Парсит информацию о фото для серии автомобиля.
    Извлекает цвета (interior/exterior) и категории фото.
    """
    result = payload.get("result") or {}
    
    colors: List[PhotoColorData] = []
    categories: List[PhotoCategoryData] = []
    
    # Парсим цвета интерьера
    interior_colors = result.get("interiorcolor") or []
    for color in interior_colors:
        color_id = color.get("id")
        if color_id is None:
            continue
        colors.append(
            PhotoColorData(
                id=int(color_id),
                series_id=series_id,
                color_type="interior",
                name=str(color.get("name") or ""),
                value=color.get("value"),

                isonsale=bool(color.get("isonsale")) if color.get("isonsale") is not None else None,
            )
        )
    
    # Парсим цвета экстерьера
    exterior_colors = result.get("exteriorcolor") or []
    for color in exterior_colors:
        color_id = color.get("id")
        if color_id is None:
            continue
        colors.append(
            PhotoColorData(
                id=int(color_id),
                series_id=series_id,
                color_type="exterior",
                name=str(color.get("name") or ""),
                value=color.get("value"),

                isonsale=bool(color.get("isonsale")) if color.get("isonsale") is not None else None,
            )
        )
    
    # Парсим категории фото
    pictypelist = result.get("pictypelist") or []
    for category in pictypelist:
        category_id = category.get("id")
        if category_id is None:
            continue
        categories.append(
            PhotoCategoryData(
                id=int(category_id),
                series_id=series_id,
                name=str(category.get("name") or ""),
            )
        )
    
    return {"colors": colors, "categories": categories}


def parse_pic_list(
    payload: Dict[str, Any],
    series_id: int,
    spec_id: int,
    category_id: int,
    color_id: int,
) -> Dict[str, Any]:
    """
    Парсит список фотографий из ответа API.
    
    Returns:
        Словарь с ключами:
        - "photos": список PhotoData
        - "pagecount": общее количество страниц
        - "rowcount": общее количество фото
    """
    result = payload.get("result") or {}
    piclist = result.get("piclist") or []
    
    photos: List[PhotoData] = []
    for pic in piclist:
        pic_id = pic.get("id")
        if not pic_id:
            continue
        
        # Используем colorid из ответа API, если он есть и не равен 0
        # Иначе используем color_id из параметров запроса
        pic_colorid = pic.get("colorid", 0)
        final_color_id = pic_colorid if pic_colorid and pic_colorid != 0 else color_id
        
        # Используем specid из ответа API, если он есть
        # Иначе используем spec_id из параметров запроса
        pic_specid = pic.get("specid")
        final_spec_id = int(pic_specid) if pic_specid else spec_id
        
        photos.append(
            PhotoData(
                id=str(pic_id),
                series_id=series_id,
                specification_id=final_spec_id,
                category_id=category_id,
                color_id=final_color_id,
                originalpic=pic.get("originalpic"),

                specname=pic.get("specname"),
            )
        )
    
    return {
        "photos": photos,
        "pagecount": result.get("pagecount", 0),
        "rowcount": result.get("rowcount", 0),
    }


def parse_pano_baseinfo(payload: Dict[str, Any], spec_id: int) -> Dict[str, Any]:
    """
    Парсит базовую информацию о панораме, включая список цветов для 360 фото.
    
    Args:
        payload: JSON ответ от get_pano_baseinfo
        spec_id: ID комплектации
    
    Returns:
        Словарь с ключами:
        - "ext_id": ID панорамы (ext.Id)
        - "colors": список PanoramaColorData
        - "photos": список PanoramaPhotoData (если есть в ответе)
    """
    ext = payload.get("ext") or {}
    ext_id = ext.get("Id")
    image_root = payload.get("image_root", "//panovr.autoimg.cn/pano")
    
    colors: List[PanoramaColorData] = []
    photos: List[PanoramaPhotoData] = []
    color_info_list = payload.get("color_info") or []
    
    for color_info in color_info_list:
        color_id = color_info.get("ColorId")
        if color_id is None:
            continue
        
        color_data = PanoramaColorData(
            id=int(color_info.get("Id", 0)),
            spec_id=spec_id,
            ext_id=ext_id,
            base_color_name=color_info.get("BaseColorName"),
            color_name=str(color_info.get("ColorName", "")),
            color_value=color_info.get("ColorValue"),
            color_id=int(color_id),
        )
        colors.append(color_data)
        
        # Парсим фото из Hori.Normal (если есть)
        hori = color_info.get("Hori") or {}
        normal_photos = hori.get("Normal") or []
        
        for photo_item in normal_photos:
            seq = photo_item.get("Seq")
            url_path = photo_item.get("Url")
            
            if seq is None or not url_path:
                continue
            
            # Формируем полный URL
            if url_path.startswith("http"):
                full_url = url_path
            else:
                # Добавляем базовый путь, если URL относительный
                # URL может быть вида: g33/M02/5D/F9/1200x0_autohomecar__ChxpVmlLnCuAd-9DACogVgSLl8g598.png.png
                if url_path.startswith("g") and "/" in url_path:
                    # Это путь вида g33/M02/5D/F9/...
                    full_url = f"https:{image_root}/{url_path}"
                elif not url_path.startswith("/"):
                    url_path = "/" + url_path
                    full_url = f"https:{image_root}{url_path}"
                else:
                    full_url = f"https:{image_root}{url_path}"
            
            # Формируем уникальный ID: spec_id_color_id_seq
            photo_id = f"{spec_id}_{color_id}_{seq}"
            
            photos.append(
                PanoramaPhotoData(
                    id=photo_id,
                    spec_id=spec_id,
                    color_id=color_id,
                    seq=int(seq),
                    url=full_url,
                )
            )
    
    return {
        "ext_id": ext_id,
        "colors": colors,
        "photos": photos,
    }


def parse_vr_info(
    payload: Dict[str, Any],
    spec_id: int,
    color_id: int,
) -> List[PanoramaPhotoData]:
    """
    Парсит список 360-градусных фото из ответа getVrInfo.
    
    Args:
        payload: JSON ответ от get_vr_info
        spec_id: ID комплектации
        color_id: ID цвета
    
    Returns:
        Список PanoramaPhotoData
    """
    result = payload.get("result") or {}
    photo_list = result.get("l1") or []
    
    photos: List[PanoramaPhotoData] = []
    for photo_item in photo_list:
        seq = photo_item.get("seq")
        url = photo_item.get("url")
        
        if seq is None or not url:
            continue
        
        # Формируем уникальный ID: spec_id_color_id_seq
        photo_id = f"{spec_id}_{color_id}_{seq}"
        
        photos.append(
            PanoramaPhotoData(
                id=photo_id,
                spec_id=spec_id,
                color_id=color_id,
                seq=int(seq),
                url=str(url),
            )
        )
    
    return photos
