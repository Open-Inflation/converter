from __future__ import annotations

import logging
from collections.abc import Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


LOGGER = logging.getLogger(__name__)


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


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

        LOGGER.info(
            "Storage HTTP adapter configured: origin=%s timeout_seconds=%.1f fail_on_error=%s",
            self._origin,
            self._timeout_seconds,
            self._fail_on_error,
        )

    def delete_images(self, urls: Sequence[str]) -> None:
        image_names = self._extract_unique_image_names(urls)
        LOGGER.debug(
            "Storage delete_images requested: urls=%s deletable_unique_images=%s",
            len(urls),
            len(image_names),
        )
        for image_name in image_names:
            self._delete_one(image_name)

    def persist_images(self, urls: Sequence[str]) -> list[str]:
        out: list[str] = []
        cache: dict[str, str] = {}
        for raw_url in urls:
            token = str(raw_url).strip()
            if not token:
                out.append(token)
                continue

            image_name = self._image_name_from_url(token)
            if image_name is None:
                out.append(token)
                continue

            if self._is_permanent_path(token):
                out.append(token)
                continue

            persisted_url = cache.get(image_name)
            if persisted_url is None:
                persisted_url = self._persist_one(image_name=image_name, fallback_url=token)
                cache[image_name] = persisted_url
            out.append(persisted_url)
        return out

    def get_image_sizes(self, urls: Sequence[str]) -> list[int | None]:
        out: list[int | None] = []
        cache: dict[str, int | None] = {}
        for raw_url in urls:
            head_url = self._head_url_from_asset_url(raw_url)
            if head_url is None:
                out.append(None)
                continue
            if head_url not in cache:
                cache[head_url] = self._head_one(head_url)
            out.append(cache[head_url])
        return out

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
        elif clean_path.startswith("/images-permanent/"):
            image_name = clean_path.removeprefix("/images-permanent/")
        elif clean_path.startswith("/images_permanent/"):
            image_name = clean_path.removeprefix("/images_permanent/")
        elif clean_path.startswith("images/"):
            image_name = clean_path.removeprefix("images/")
        elif clean_path.startswith("images-permanent/"):
            image_name = clean_path.removeprefix("images-permanent/")
        elif clean_path.startswith("images_permanent/"):
            image_name = clean_path.removeprefix("images_permanent/")
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
        url = f"{self._base_url}/api/images/{encoded}?scope=both"

        request = Request(
            url=url,
            method="DELETE",
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 204))
                if status == 204:
                    LOGGER.debug("Storage image deleted: image=%s status=%s", image_name, status)
                    return
                if status == 404:
                    LOGGER.debug("Storage image already absent: image=%s status=%s", image_name, status)
                    return
                raise RuntimeError(f"Storage delete failed for {image_name}: HTTP {status}")
        except HTTPError as exc:
            if int(exc.code) == 404:
                LOGGER.debug("Storage image already absent: image=%s status=%s", image_name, int(exc.code))
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

    def _persist_one(self, *, image_name: str, fallback_url: str) -> str:
        encoded = quote(image_name, safe="")
        url = f"{self._base_url}/api/images/{encoded}/persist"
        request = Request(
            url=url,
            method="POST",
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        opener = build_opener(_NoRedirectHandler())
        try:
            with opener.open(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 303))
                location = str(getattr(response, "headers", {}).get("Location") or "").strip()
                if status == 303 and location:
                    persisted_url = urljoin(self._origin, location)
                    LOGGER.debug(
                        "Storage image persisted: image=%s status=%s persisted_url=%s",
                        image_name,
                        status,
                        persisted_url,
                    )
                    return persisted_url
                if status in {404, 409}:
                    LOGGER.debug(
                        "Storage image persist skipped: image=%s status=%s fallback=true",
                        image_name,
                        status,
                    )
                    return fallback_url
                message = f"Storage persist failed for {image_name}: HTTP {status}"
                if self._fail_on_error:
                    raise RuntimeError(message)
                LOGGER.warning(message)
                return fallback_url
        except HTTPError as exc:
            if int(exc.code) == 303:
                location = str((exc.headers or {}).get("Location") or "").strip()
                if location:
                    return urljoin(self._origin, location)
                return fallback_url
            if int(exc.code) in {404, 409}:
                LOGGER.debug(
                    "Storage image persist skipped: image=%s status=%s fallback=true",
                    image_name,
                    int(exc.code),
                )
                return fallback_url
            message = f"Storage persist failed for {image_name}: HTTP {exc.code}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
            return fallback_url
        except URLError as exc:
            message = f"Storage persist failed for {image_name}: {exc}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
            return fallback_url

    def _head_url_from_asset_url(self, url: str) -> str | None:
        token = str(url).strip()
        if not token:
            return None
        image_name = self._image_name_from_url(token)
        if image_name is None:
            return None
        encoded = quote(image_name, safe="")
        base_path = "/images-permanent/" if self._is_permanent_path(token) else "/images/"
        return f"{self._origin}{base_path}{encoded}"

    def _head_one(self, url: str) -> int | None:
        request = Request(url=url, method="HEAD")
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 200))
                if status != 200:
                    message = f"Storage HEAD failed for {url}: HTTP {status}"
                    if self._fail_on_error:
                        raise RuntimeError(message)
                    LOGGER.warning(message)
                    return None
                raw_length = str((getattr(response, "headers", {}) or {}).get("Content-Length") or "").strip()
                if not raw_length:
                    return None
                try:
                    parsed = int(raw_length)
                except ValueError:
                    return None
                return parsed if parsed >= 0 else None
        except HTTPError as exc:
            if int(exc.code) in {404, 405}:
                LOGGER.debug("Storage HEAD size unavailable: url=%s status=%s", url, int(exc.code))
                return None
            message = f"Storage HEAD failed for {url}: HTTP {exc.code}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
            return None
        except URLError as exc:
            message = f"Storage HEAD failed for {url}: {exc}"
            if self._fail_on_error:
                raise RuntimeError(message) from exc
            LOGGER.warning(message)
            return None

    def _is_permanent_path(self, url: str) -> bool:
        token = str(url).strip()
        if not token:
            return False
        parsed = urlparse(token)
        path = parsed.path if parsed.scheme and parsed.netloc else token
        normalized = path.strip()
        return (
            normalized.startswith("/images-permanent/")
            or normalized.startswith("images-permanent/")
            or normalized.startswith("/images_permanent/")
            or normalized.startswith("images_permanent/")
        )
