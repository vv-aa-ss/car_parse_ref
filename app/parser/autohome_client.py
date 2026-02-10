import json
import time
from typing import Any, Dict

import requests


class AutohomeClient:
    def __init__(self, timeout: float = 3.0):
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )

    def get_tree_menu(self) -> Dict[str, Any]:
        url = "https://www.autohome.com.cn/web-main/car/web/price/treeMenu"
        resp = self._session.get(url, params={"extendseries": 1}, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def get_param_conf(self, series_id: int, mode: int = 1) -> Dict[str, Any]:
        url = "https://www.autohome.com.cn/web-main/car/param/getParamConf"
        resp = self._session.get(
            url,
            params={"mode": mode, "site": 1, "seriesid": series_id},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_series_base_pic_info(self, series_id: int) -> Dict[str, Any]:
        url = "https://www.autohome.com.cn/web-main/car/series/getseriesbasepicinforequest"
        resp = self._session.get(
            url,
            params={"seriesid": series_id},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_pic_list(
        self,
        series_id: int,
        spec_id: int,
        category_id: int,
        color_id: int,
        is_inner: bool,
        page_size: int,
        page_index: int = 1,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Получает список фотографий с повторными попытками при ошибках сети.
        
        Args:
            series_id: ID серии автомобиля
            spec_id: ID комплектации
            category_id: ID категории фото
            color_id: ID цвета (0 если не указан)
            is_inner: True для интерьера, False для экстерьера
            page_size: Размер страницы
            page_index: Номер страницы (начинается с 1)
            max_retries: Максимальное количество повторных попыток
        """
        url = "https://car.app.autohome.com.cn/carbase/pic/getPicList"
        params = {
            "pluginversion": "11.65.1",
            "pm": 1,
            "seriesid": series_id,
            "specid": spec_id,
            "categoryid": category_id,
            "colorid": color_id,
            "isinner": 1 if is_inner else 0,
            "pagesize": page_size,
            "pageindex": page_index,
        }
        
        last_error = None
        for attempt in range(max_retries):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 0.5  # Экспоненциальная задержка: 0.5, 1.0, 1.5 сек
                    time.sleep(wait_time)
                else:
                    raise

    def get_pano_page(self, spec_id: int) -> str:
        """
        Получает HTML страницу панорамы для указанного spec_id.
        Используется для извлечения ext_id из страницы.
        
        Args:
            spec_id: ID комплектации
        
        Returns:
            HTML содержимое страницы
        """
        url = "https://pano.autohome.com.cn/car/ext/{}".format(spec_id)
        resp = self._session.get(url, timeout=self._timeout)
        resp.raise_for_status()
        return resp.text

    def get_pano_baseinfo(self, ext_id: int) -> Dict[str, Any]:
        """
        Получает базовую информацию о панораме, включая список цветов для 360 фото.
        
        Args:
            ext_id: ID панорамы (ext ID). Можно получить из HTML страницы или попробовать spec_id.
        
        Returns:
            JSON ответ с информацией о панораме и цветах
        """
        url = "https://pano.autohome.com.cn/api/ext/baseinfo/{}".format(ext_id)
        resp = self._session.get(
            url,
            params={"src": "m", "category": "car", "deviceId": "", "cityId": "110100"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_vr_info(
        self,
        spec_id: int,
        color_id: int,
    ) -> Dict[str, Any]:
        """
        Получает список 360-градусных фото для указанной комплектации и цвета.
        
        Args:
            spec_id: ID комплектации
            color_id: ID цвета (из color_info из get_pano_baseinfo)
        
        Returns:
            JSON ответ со списком фото 360
        """
        url = "https://www.autohome.com.cn/web-main/car/series/getVrInfo"
        resp = self._session.get(
            url,
            params={
                "category": "car",
                "angle": "hori",
                "sizelevel": "l1",
                "_appid": "pc",
                "specid": spec_id,
                "colorid": color_id,
            },
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
