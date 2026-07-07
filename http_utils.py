import time
import requests


def fetch_with_retry(url: str, headers: dict, params: dict = None,
                      timeout: int = 10, max_retries: int = 1) -> requests.Response:
    """GET with exponential backoff on transient failures (1 retry, 1 s delay)."""
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(2 ** attempt)
    raise last_exc
