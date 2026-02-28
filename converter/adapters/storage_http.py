from __future__ import annotations

import logging
from collections.abc import Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)


class StorageHTTPRepository:
    def __init__(
        self,
        *,
        base_url: str,
        api_token: str,
        timeout_seconds: float = 10.0,
        fail_on_error: bool = False,
    ) -> None:
        token = base_url.strip().rstrip("/")
        parsed = urlparse(token)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("storage base_url must be a valid http(s) URL")

        self._base_url = token
        self._origin = f"{parsed.scheme}://{parsed.netloc}"
        self._api_token = api_token.strip()
        self._timeout_seconds = max(0.1, float(timeout_seconds))
        self._fail_on_error = bool(fail_on_error)

        if not self._api_token:
            raise ValueError("storage api_token must be non-empty")

    def delete_images(self, urls: Sequence[str]) -> None:
        image_names = self._extract_unique_image_names(urls)
        for image_name in image_names:
            self._delete_one(image_name)

    def _extract_unique_image_names(self, urls: Sequence[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for url in urls:
            image_name = self._image_name_from_url(url)
            if image_name is None or image_name in seen:
                continue
            seen.add(image_name)
            out.append(image_name)
        return out

    def _image_name_from_url(self, url: str) -> str | None:
        token = str(url).strip()
        if not token:
            return None

        parsed = urlparse(token)
        path = token
        if parsed.scheme and parsed.netloc:
            origin = f"{parsed.scheme}://{parsed.netloc}"
            if origin != self._origin:
                return None
            path = parsed.path

        clean_path = path.strip()
        if clean_path.startswith("/api/images/"):
            image_name = clean_path.removeprefix("/api/images/")
        elif clean_path.startswith("/images/"):
            image_name = clean_path.removeprefix("/images/")
        elif clean_path.startswith("images/"):
            image_name = clean_path.removeprefix("images/")
        else:
            return None

        image_name = unquote(image_name).strip().lstrip("/")
        if not image_name:
            return None
        if "/" in image_name or "\\" in image_name:
            return None
        if ".." in image_name:
            return None
        return image_name

    def _delete_one(self, image_name: str) -> None:
        encoded = quote(image_name, safe="")
        url = f"{self._base_url}/api/images/{encoded}"

        request = Request(
            url=url,
            method="DELETE",
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 204))
                if status == 204:
                    return
                if status == 404:
                    return
                raise RuntimeError(f"Storage delete failed for {image_name}: HTTP {status}")
        except HTTPError as exc:
            if int(exc.code) == 404:
                return
            message = f"Storage delete failed for {image_name}: HTTP {exc.code}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
        except URLError as exc:
            message = f"Storage delete failed for {image_name}: {exc}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
