import json
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

    def get_param_conf(self, series_id: int) -> Dict[str, Any]:
        url = "https://www.autohome.com.cn/web-main/car/param/getParamConf"
        resp = self._session.get(
            url,
            params={"mode": 1, "site": 1, "seriesid": series_id},
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
