from abc import (
    ABC,
    abstractmethod,
)
from pathlib import Path
import subprocess

import certifi
import requests
from playwright.sync_api import sync_playwright


class ContentDownloader(ABC):
    def __init__(
        self,
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
    ):
        self.user_agent = user_agent

    def download(
        self,
        url: str,
        *,
        dest_folder: str = ".",
        file_name: str | None = None,
        timeout: int | None = None,
    ) -> Path:
        dest_folder: Path = self._mkdir(dest_folder)
        file_name: str = file_name or url.split("/")[-1]

        # TODO не определенная ошибка
        if not file_name:
            raise ValueError

        """
        TODO написать определитель расширения (.pdf, .html, ...) по Content-Type, 
        и это расширение будет зафиксировано в file_path
        
        Возможно этот определитель будет написан вне этих классов, и будет отдаваться вместе с UUID-названием.
        Либо написать какой-то менеджер Downloader-ов, который будет как раз-таки определять тип получаемого содержимого и
        будет адресовать это нужным Downloader-ам. [[DownloadPool]]
        """
        file_path: Path = dest_folder / file_name
        self._download(
            url=url,
            file_path=file_path,
            timeout=timeout,
        )

        return file_path

    @abstractmethod
    def _download(
        self,
        url: str,
        file_path: Path,
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
        file_path: Path,
        timeout: int | None,
    ) -> None:
        chunk_size: int = 8192 if self._is_large_file(url) else 0
        with requests.get(
            url,
            headers={"User-Agent": self.user_agent},
            stream=True,
            timeout=timeout,
            verify=certifi.where(),
        ) as request:
            request.raise_for_status()
            with open(file_path, "wb") as file:
                if chunk_size:
                    _ = (file.write(chunk) for chunk in request.iter_content(chunk_size=chunk_size) if chunk)
                else:
                    file.write(request.content)

    def _is_large_file(self, url: str, threshold_bytes: int = 10_000_000) -> bool:
        try:
            request = requests.head(url, allow_redirects=True, timeout=5)
            size = request.headers.get("Content-Length")
            if size is None:
                return True
            return int(size) >= threshold_bytes
        except requests.RequestException:
            return True


class WgetDocumentDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: Path,
        timeout: int | None,
    ) -> None:
        cmd = ["wget", url, "-O", str(file_path), "--timeout", str(timeout), "--tries", "3"]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            # TODO не определенная ошибка
            # print(f"Ошибка при загрузке {url}: {e}")
            raise
        raise


class RequestsPageDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: Path,
        timeout: int | None,
    ) -> None:
        with requests.get(
            url,
            headers={"User-Agent": self.user_agent},
            timeout=timeout,
            verify=certifi.where(),
        ) as request:
            request.raise_for_status()
            with open(file_path, "wb") as file:
                file.write(request.content)


class PlaywrightPageDownloader(ContentDownloader):
    def _download(
        self,
        url: str,
        file_path: Path,
        timeout: int | None,
    ) -> None:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(url)
            page.wait_for_load_state("networkidle")
            with open(file_path, "w") as file:
                file.write(page.content())
            browser.close()
