import json
import time
from os import getenv
from pathlib import Path
from typing import Any, Optional

import requests
from requests import HTTPError
from requests.exceptions import ConnectionError, Timeout
from dotenv import load_dotenv


class AmoCRMClient:
    """Клиент amoCRM API v4 с автоматическим получением и обновлением токенов."""

    def __init__(
        self,
        env_path: str = ".env",
        tokens_path: str = "tokens.json",
        request_delay: Optional[float] = None,
    ) -> None:
        self.base_dir = Path(__file__).resolve().parent
        self.env_path = self.base_dir / env_path
        self.tokens_path = Path(getenv("TOKENS_PATH", "") or self.base_dir / tokens_path)
        if request_delay is None:
            request_delay = float(getenv("AMO_REQUEST_DELAY", "0.05"))
        self.request_delay = request_delay
        self.session = requests.Session()

        load_dotenv(self.env_path)

        self.client_id = self._require_env("CLIENT_ID")
        self.client_secret = self._require_env("CLIENT_SECRET")
        self.redirect_uri = self._require_env("REDIRECT_URI")
        self.subdomain = self._normalize_subdomain(self._require_env("SUBDOMAIN"))
        if self.subdomain.startswith("api-"):
            raise ValueError(
                "В SUBDOMAIN нужно указать поддомен аккаунта amoCRM, а не API-домен. "
                "Например, для https://mycompany.amocrm.ru укажите SUBDOMAIN=mycompany."
            )
        self.auth_code = getenv("AUTH_CODE", "").strip()
        self.base_url = f"https://{self.subdomain}.amocrm.ru"

        self.tokens = self._load_or_create_tokens()

    def get(self, endpoint: str, params: Optional[list[tuple[str, Any]]] = None) -> dict[str, Any]:
        """Выполняет GET-запрос с учетом лимитов и автообновлением токена."""
        return self._request("GET", endpoint, params=params)

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[list[tuple[str, Any]]] = None,
        retry_after_refresh: bool = True,
    ) -> dict[str, Any]:
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self.tokens['access_token']}"}

        # amoCRM чувствителен к частым запросам, поэтому держим небольшой интервал.
        time.sleep(self.request_delay)

        response = self._send_with_retries(
            method,
            url,
            headers=headers,
            params=params,
        )

        if response.status_code == 401 and retry_after_refresh:
            self._refresh_tokens()
            return self._request(method, endpoint, params=params, retry_after_refresh=False)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
            return self._request(method, endpoint, params=params, retry_after_refresh=retry_after_refresh)

        self._raise_for_status(response)
        if not response.content:
            return {}

        return response.json()

    def _send_with_retries(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        params: Optional[list[tuple[str, Any]]] = None,
    ) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, 5):
            try:
                return self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    timeout=60,
                )
            except (ConnectionError, Timeout) as exc:
                last_error = exc
                # amoCRM иногда закрывает длинные выгрузки без ответа. Повторяем запрос с паузой.
                time.sleep(min(2 ** attempt, 10))

        raise RuntimeError(f"amoCRM не ответила после нескольких попыток: {last_error}") from last_error

    def _load_or_create_tokens(self) -> dict[str, Any]:
        tokens = self._load_tokens()
        if tokens.get("access_token") and tokens.get("refresh_token"):
            return tokens

        env_tokens = {
            "access_token": getenv("ACCESS_TOKEN", "").strip(),
            "refresh_token": getenv("REFRESH_TOKEN", "").strip(),
            "token_type": "Bearer",
        }
        if env_tokens["access_token"] and env_tokens["refresh_token"]:
            self._save_tokens(env_tokens)
            return env_tokens

        return self._exchange_auth_code()

    def _load_tokens(self) -> dict[str, Any]:
        if not self.tokens_path.exists():
            return {}

        try:
            return json.loads(self.tokens_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_tokens(self, tokens: dict[str, Any]) -> None:
        self.tokens_path.parent.mkdir(parents=True, exist_ok=True)
        self.tokens_path.write_text(
            json.dumps(tokens, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _exchange_auth_code(self) -> dict[str, Any]:
        if not self.auth_code:
            raise ValueError("Файл tokens.json не найден, а AUTH_CODE в .env не заполнен")

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": self.auth_code,
            "redirect_uri": self.redirect_uri,
        }
        tokens = self._token_request(payload)
        self._save_tokens(tokens)
        return tokens

    def _refresh_tokens(self) -> None:
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.tokens["refresh_token"],
            "redirect_uri": self.redirect_uri,
        }
        self.tokens = self._token_request(payload)
        self._save_tokens(self.tokens)

    def _token_request(self, payload: dict[str, str]) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/oauth2/access_token",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except HTTPError as exc:
            details = response.text.strip()
            message = f"{exc}. Ответ amoCRM: {details}" if details else str(exc)
            raise HTTPError(message, response=response) from exc

    @staticmethod
    def _require_env(name: str) -> str:
        value = getenv(name, "").strip()
        if not value:
            raise ValueError(f"Заполните переменную {name} в .env")
        return value

    @staticmethod
    def _normalize_subdomain(subdomain: str) -> str:
        clean_subdomain = subdomain.replace("https://", "").replace("http://", "")
        return clean_subdomain.strip("/").removesuffix(".amocrm.ru")
