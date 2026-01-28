from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class BrandData:
    id: int
    name: str
    logo_url: Optional[str]
    series_count: Optional[int]
    first_letter: Optional[str]


@dataclass(frozen=True)
class FactoryData:
    id: int
    brand_id: int
    name: str
    real_brand_id: Optional[int]


@dataclass(frozen=True)
class SeriesData:
    id: int
    brand_id: int
    factory_id: Optional[int]
    name: str
    state: Optional[int]
    is_new_energy: Optional[bool]
    spec_count: Optional[int]


@dataclass(frozen=True)
class SpecData:
    id: int
    series_id: int
    name: str
    spec_status: Optional[int]
    year: Optional[str]
    min_price: Optional[str]
    dealer_price: Optional[str]
    condition: Optional[Dict[str, Any]]
    sort: Optional[int]


@dataclass(frozen=True)
class ParamTitleData:
    item_id: int
    title_id: int
    item_name: str
    group_name: Optional[str]
    item_type: Optional[str]
    sort: Optional[int]
    baike_url: Optional[str]
    baike_id: Optional[int]


@dataclass(frozen=True)
class ParamValueData:
    spec_id: int
    title_id: int
    item_name: str
    sub_name: Optional[str]
    value: Optional[str]
    price_info: Optional[str]
    video_url: Optional[str]
    color_info: Optional[Dict[str, Any]]
    raw: Dict[str, Any]


def parse_tree_menu(payload: Dict[str, Any]) -> Dict[str, List[Any]]:
    result = payload.get("result") or []
    brands: List[BrandData] = []
    factories: List[FactoryData] = []
    series_list: List[SeriesData] = []

    for letter_group in result:
        first_letter = letter_group.get("firstletter")
        for brand in letter_group.get("branditems") or []:
            brand_id = brand.get("id")
            if brand_id is None:
                continue
            brands.append(
                BrandData(
                    id=int(brand_id),
                    name=str(brand.get("name") or ""),
                    logo_url=brand.get("logo"),
                    series_count=brand.get("seriescount"),
                    first_letter=first_letter,
                )
            )
            for factory in brand.get("fctitems") or []:
                factory_id = factory.get("id")
                if factory_id is None:
                    continue
                factories.append(
                    FactoryData(
                        id=int(factory_id),
                        brand_id=int(brand_id),
                        name=str(factory.get("name") or ""),
                        real_brand_id=factory.get("realbid"),
                    )
                )
                for series in factory.get("seriesitems") or []:
                    series_id = series.get("id")
                    if series_id is None:
                        continue
                    series_list.append(
                        SeriesData(
                            id=int(series_id),
                            brand_id=int(brand_id),
                            factory_id=int(factory_id),
                            name=str(series.get("name") or ""),
                            state=series.get("state"),
                            is_new_energy=bool(series.get("isnewenergy"))
                            if series.get("isnewenergy") is not None
                            else None,
                            spec_count=series.get("speccount"),
                        )
                    )

    return {"brands": brands, "factories": factories, "series": series_list}


def parse_param_conf(payload: Dict[str, Any], series_id: int) -> Dict[str, List[Any]]:
    result = payload.get("result") or {}
    titlelist = result.get("titlelist") or []
    datalist = result.get("datalist") or []

    titles: List[ParamTitleData] = []
    for group in titlelist:
        group_name = group.get("groupname")
        item_type = group.get("itemtype")
        sort = group.get("sort")
        for item in group.get("items") or []:
            item_id = item.get("itemid")
            title_id = item.get("titleid")
            if item_id is None or title_id is None:
                continue
            titles.append(
                ParamTitleData(
                    item_id=int(item_id),
                    title_id=int(title_id),
                    item_name=str(item.get("itemname") or ""),
                    group_name=group_name,
                    item_type=item_type,
                    sort=sort,
                    baike_url=item.get("baikeurl"),
                    baike_id=item.get("baikeid"),
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
                spec_status=spec_item.get("specstatus"),
                year=str(spec_item.get("year")) if spec_item.get("year") is not None else None,
                min_price=spec_item.get("minprice"),
                dealer_price=spec_item.get("dealerprice"),
                condition=spec_item.get("condition"),
                sort=spec_item.get("sort"),
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
                            spec_id=spec_id_int,
                            title_id=int(title_id),
                            item_name=item_name,
                            sub_name=sub.get("name") or "",
                            value=sub.get("value"),
                            price_info=sub.get("priceinfo") or conf.get("priceinfo"),
                            video_url=conf.get("videourl"),
                            color_info=conf.get("colorinfo"),
                            raw=conf,
                        )
                    )
            else:
                # Нет подсписка - значение находится в conf.itemname
                # В этом случае itemname из conf - это значение характеристики
                value = conf.get("itemname")
                
                param_values.append(
                    ParamValueData(
                        spec_id=spec_id_int,
                        title_id=int(title_id),
                        item_name=item_name,  # Название из titlelist
                        sub_name="",  # Пустая строка вместо None
                        value=value,  # Значение из conf.itemname
                        price_info=conf.get("priceinfo"),
                        video_url=conf.get("videourl"),
                        color_info=conf.get("colorinfo"),
                        raw=conf,
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
