from abc import (
    ABC,
    abstractmethod,
)
from pathlib import Path
from typing import Literal
import subprocess
import logging

import certifi
import requests
from playwright.sync_api import sync_playwright

import robotparser


class ContentDownloader(ABC):
    """
    Базовый класс загрузчика, который имеет основной метод download и абстрактный _download.
    Основной метод обрабатывает общие исключения и возможные ситуации, и также вызывает метод _download,
    который обязательно должен быть реализован в наследниках, где способ загрузки может быть любым.

    Однако не учтен момент, что между наследниками и базовым классом нет соглашения о полях self.url и
    self.file_size_bytes.
    """
    def __init__(
        self,
        *,
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
        dest_folder: str = ".",
    ):
        self.user_agent: str = user_agent
        self.dest_folder: str = dest_folder
        self.url: str = ""
        self.download_status: Literal["success", "failed_download", "skipped_robots"] = "success"
        self.error_message: str = ""
        self.file_path: str = ""
        self.file_size_bytes: int = 0

    def download(
        self,
        url: str,
        *,
        dest_folder: str | None = None,
        file_name: str | None = None,
        timeout: int | None = 5,
    ) -> str:
        dest_folder: Path = self._mkdir(dest_folder or self.dest_folder)
        file_name: str = file_name or url.split("/")[-1]

        if not file_name:
            self.download_status = "failed_download"
            raise ValueError("The url cannot be empty")
        if not robotparser.can_fetch(url, self.user_agent):
            logging.warning(f"robots.txt disallows accessing by {url}")
            self.download_status = "skipped_robots"

        file_path: str = str(dest_folder / file_name)
        try:
            logging.info(f"Starting new download {url}")
            self._download(
                url=url,
                file_path=file_path,
                timeout=timeout,
            )
        except Exception as e:
            logging.warning(f"Download failed: {e}")
            self.download_status = "failed_download"
            self.error_message = str(e)

        self.file_path = file_path
        return self.file_path

    @abstractmethod
    def _download(
        self,
        url: str,
        file_path: str,
        timeout: int | None,
    ) -> None: ...

    def _mkdir(self, folder: str) -> Path:
        folder: Path = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        return folder


class RequestsDocumentDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: str,
        timeout: int | None,
    ) -> None:
        with requests.get(
            url,
            headers={"User-Agent": self.user_agent},
            timeout=timeout,
            verify=certifi.where(),
        ) as request:
            request.raise_for_status()

            self.url = request.url
            self.file_size_bytes = request.headers.get("Content-Length", 0)

            with open(file_path, "wb") as file:
                file.write(request.content)

class WgetDocumentDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: str,
        timeout: int | None,
    ) -> None:
        cmd = ["wget", url, "-O", file_path, "--timeout", str(timeout), "--tries", "3"]
        subprocess.run(cmd, check=True)


class RequestsPageDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: str,
        timeout: int | None,
    ) -> None:
        with requests.get(
            url,
            headers={"User-Agent": self.user_agent},
            timeout=timeout,
            verify=certifi.where(),
        ) as request:
            request.raise_for_status()

            self.url = request.url
            self.file_size_bytes = request.headers.get("Content-Length", 0)

            with open(file_path, "wb") as file:
                file.write(request.content)


class PlaywrightPageDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: str,
        timeout: int | None,
    ) -> None:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            response = page.goto(url, timeout=timeout * 1000, wait_until="networkidle")

            self.url = response.url
            self.file_size_bytes = response.headers.get("content-length", 0)

            with open(file_path, "w") as file:
                file.write(page.content())
            browser.close()
