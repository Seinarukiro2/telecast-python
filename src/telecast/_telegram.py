"""Thin Telegram Bot API client."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass
class APIError(Exception):
    """Telegram API error."""

    error_code: int
    description: str
    retry_after: int | None = None

    def __str__(self) -> str:
        s = f"Telegram {self.error_code}: {self.description}"
        if self.retry_after:
            s += f" (retry_after={self.retry_after})"
        return s

    @property
    def is_permanent(self) -> bool:
        return self.error_code in (400, 401, 403, 404)

    @property
    def is_rate_limited(self) -> bool:
        return self.error_code == 429


def _check(resp: httpx.Response) -> dict[str, Any]:
    data = resp.json()
    if not data.get("ok"):
        params = data.get("parameters", {})
        raise APIError(
            error_code=data.get("error_code", resp.status_code),
            description=data.get("description", "unknown error"),
            retry_after=params.get("retry_after"),
        )
    return data


class TelegramClient:
    """Synchronous Telegram Bot API client."""

    def __init__(self, token: str, base_url: str = "") -> None:
        if not base_url:
            base_url = "https://api.telegram.org"
        self._base = f"{base_url.rstrip('/')}/bot{token}"
        self._http = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self._http.close()

    def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        parse_mode: str = "",
        disable_web_page_preview: bool = False,
        disable_notification: bool = False,
        reply_markup: str = "",
        reply_to_message_id: int = 0,
    ) -> int:
        """Send a text message. Returns the message_id."""
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if disable_web_page_preview:
            payload["disable_web_page_preview"] = True
        if disable_notification:
            payload["disable_notification"] = True
        if reply_markup:
            payload["reply_markup"] = json.loads(reply_markup)
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id

        data = _check(self._http.post(f"{self._base}/sendMessage", json=payload))
        return data["result"]["message_id"]

    def edit_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        parse_mode: str = "",
        disable_web_page_preview: bool = False,
        reply_markup: str = "",
    ) -> None:
        """Edit a message text."""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if disable_web_page_preview:
            payload["disable_web_page_preview"] = True
        if reply_markup:
            payload["reply_markup"] = json.loads(reply_markup)

        _check(self._http.post(f"{self._base}/editMessageText", json=payload))

    def send_photo(
        self,
        chat_id: int,
        photo: str | Path,
        *,
        caption: str = "",
        parse_mode: str = "",
        disable_notification: bool = False,
        reply_markup: str = "",
        reply_to_message_id: int = 0,
    ) -> int:
        """Send a photo. `photo` can be a file_id, URL, or local path. Returns message_id."""
        path = Path(photo) if isinstance(photo, str) else photo

        if path.exists():
            # Upload file
            files = {"photo": (path.name, path.read_bytes())}
            data: dict[str, Any] = {"chat_id": str(chat_id)}
            if caption:
                data["caption"] = caption
            if parse_mode:
                data["parse_mode"] = parse_mode
            if disable_notification:
                data["disable_notification"] = "true"
            if reply_to_message_id:
                data["reply_to_message_id"] = str(reply_to_message_id)
            if reply_markup:
                data["reply_markup"] = reply_markup
            resp = _check(self._http.post(f"{self._base}/sendPhoto", data=data, files=files))
        else:
            # file_id or URL
            payload: dict[str, Any] = {"chat_id": chat_id, "photo": str(photo)}
            if caption:
                payload["caption"] = caption
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if disable_notification:
                payload["disable_notification"] = True
            if reply_to_message_id:
                payload["reply_to_message_id"] = reply_to_message_id
            if reply_markup:
                payload["reply_markup"] = json.loads(reply_markup)
            resp = _check(self._http.post(f"{self._base}/sendPhoto", json=payload))

        return resp["result"]["message_id"]

    def send_document(
        self,
        chat_id: int,
        document: str | Path,
        *,
        caption: str = "",
        parse_mode: str = "",
        disable_notification: bool = False,
        reply_markup: str = "",
        reply_to_message_id: int = 0,
    ) -> int:
        """Send a document. `document` can be a file_id, URL, or local path. Returns message_id."""
        path = Path(document) if isinstance(document, str) else document

        if path.exists():
            files = {"document": (path.name, path.read_bytes())}
            data: dict[str, Any] = {"chat_id": str(chat_id)}
            if caption:
                data["caption"] = caption
            if parse_mode:
                data["parse_mode"] = parse_mode
            if disable_notification:
                data["disable_notification"] = "true"
            if reply_to_message_id:
                data["reply_to_message_id"] = str(reply_to_message_id)
            if reply_markup:
                data["reply_markup"] = reply_markup
            resp = _check(self._http.post(f"{self._base}/sendDocument", data=data, files=files))
        else:
            payload: dict[str, Any] = {"chat_id": chat_id, "document": str(document)}
            if caption:
                payload["caption"] = caption
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if disable_notification:
                payload["disable_notification"] = True
            if reply_to_message_id:
                payload["reply_to_message_id"] = reply_to_message_id
            if reply_markup:
                payload["reply_markup"] = json.loads(reply_markup)
            resp = _check(self._http.post(f"{self._base}/sendDocument", json=payload))

        return resp["result"]["message_id"]
