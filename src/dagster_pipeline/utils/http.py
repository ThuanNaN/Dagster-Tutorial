"""HTTP client utilities with retry and timeout."""
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class HttpClientConfig:
    base_url: str
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5


class HttpClient:
    """HTTP client with exponential backoff retry."""

    def __init__(self, config: HttpClientConfig):
        self.config = config
        self.session = requests.Session()
        retry = Retry(
            total=config.max_retries,
            backoff_factor=config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.timeout = config.timeout

    def get(self, endpoint: str, params: dict | None = None) -> requests.Response:
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        logger.info("GET %s params=%s", url, params)
        start = time.time()
        response = self.session.get(url, params=params)
        duration = time.time() - start
        logger.info("GET %s completed in %.2fs status=%d", url, duration, response.status_code)
        response.raise_for_status()
        return response

    def get_json(self, endpoint: str, params: dict | None = None) -> dict[str, Any]:
        response = self.get(endpoint, params=params)
        return response.json()
