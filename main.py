import csv
import re
import uuid
import argparse
import mimetypes
from dataclasses import (
    dataclass,
    asdict,
)
from pathlib import Path
from typing import Literal

from urllib.parse import (
    urlparse,
    parse_qsl,
    urlunparse,
    urlencode,
)
import requests
from playwright.sync_api import sync_playwright

from downloader import (
    ContentDownloader,
    RequestsDocumentDownloader,
    RequestsPageDownloader,
)


"""
    id (уникальный порядковый номер или идентификатор записи)
    source_url (URL из входного CSV-файла)
    final_url (URL, с которого фактически был скачан контент, если были редиректы)
    download_timestamp (дата и время скачивания/обработки в формате YYYY-MM-DD HH:MM:SS)
    download_status (статус: success; failed_download; failed_processing; skipped_robots)
    error_message (краткое описание ошибки, если была)
    content_type_detected (определенный тип контента: document или page)
    raw_file_path (относительный путь к сохраненному сырому файлу/странице)
    processed_file_path (относительный путь к файлу с очищенным текстом)
    file_size_bytes (размер сырого файла в байтах, если применимо)
    document_page_count (количество страниц, если это документ и удалось определить)
    detected_language (определенный язык документа/страницы)
    extracted_keywords (извлеченные ключевые слова через запятую, если применимо)
    extracted_entities (опционально: извлеченные именованные сущности, если реализовывали)
    summary (опционально: краткое содержание документа, если реализовывали)
    metadata_author (автор из метаданных документа, если доступно)
    metadata_creation_date (дата создания из метаданных документа, если доступно)
"""
@dataclass
class URLMetadata:
    id: str
    source_url: str
    final_url: str
    download_timestamp: str
    download_status: Literal["success", "failed_download", "failed_processing", "skipped_robots"]
    error_message: str
    content_type_detected: Literal["document", "page"]
    raw_file_path: str
    processed_file_path: str
    file_size_bytes: int
    document_page_count: int
    detected_language: str
    extracted_keywords: list[str]
    extracted_entities: list[str]
    summary: str
    metadata_author: str
    metadata_creation_date: str


def extract_urls_from_csv_file(file_name: str) -> list[str]:
    urls: list[str] = []
    with open(file_name, "r") as file:
        csv_reader = csv.reader(file, doublequote=False)
        for row in csv_reader:
            for column in row:
                parsed_column = urlparse(column)
                if parsed_column.netloc:
                    urls.append(column)
    return urls


def clear_urls_of_garbage(
    urls: list[str],
    exclude_prefixes: set[str] | None = None,
    exclude_postfixes: set[str] | None = None,
    exclude_params: set[str] | None = None,
) -> list[str]:
    ignored_query_params_re: str = r"(^utm_|clid$|^cache_)"
    pattern: re.Pattern = re.compile(ignored_query_params_re)
    cleared_urls: list[str] = []

    for url in urls:
        parsed_url = urlparse(url)
        query_params = [(key, value) for key, value in parse_qsl(parsed_url.query) if not pattern.search(key)]
        new_parsed_url = parsed_url._replace(query=urlencode(query_params, doseq=True))
        cleared_urls.append(urlunparse(new_parsed_url))

    return cleared_urls


def define_content_type(url: str) -> str:
    # TODO добавить User-Agent, mb verify
    request: requests.Response = requests.head(url, allow_redirects=True)

    if content_type := request.headers.get("Content-Type"):
        _, sub_type = content_type.split(";")[0].split("/")
        if sub_type in ("pdf", "msword", "rtf", "json", "xml") or sub_type.startswith("vnd."):
            return "document"
        return "html"


def get_extension(content_type: str) -> str:
    content_type = content_type.split(";")[0].strip().lower()

    ext = mimetypes.guess_extension(content_type)
    if ext:
        return ext
    return ""


def download_files(urls: list[str]):
    document_downloader: ContentDownloader = RequestsDocumentDownloader()
    page_downloader: ContentDownloader = RequestsPageDownloader()
    for url in urls:
        request: requests.Response = requests.head(url, allow_redirects=True)
        content_type = request.headers.get("Content-Type")
        _, sub_type = content_type.split(";")[0].split("/")

        file_name: str = str(uuid.uuid4()) + get_extension(content_type)
        if define_content_type(url) == "document":
            document_downloader.download(url, dest_folder="raw_downloads/documents/", file_name=file_name)
        else:
            page_downloader.download(url, dest_folder="raw_downloads/pages/", file_name=file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file")
    args: argparse.Namespace = parser.parse_args()

    urls: list[str] = extract_urls_from_csv_file(args.csv_file)
    urls = clear_urls_of_garbage(urls)

    download_files(urls)


# if __name__ == "__main__":
    # page_test_dir: str = "raw_downloads/pages/"
    # file_url: str = "https://docs.netams.com/"
    #
    # my_obj = MyObj(id=10, source_url="source", final_url="final")
    # with open("123.csv", "w") as file:
    #     writer = csv.DictWriter(file, fieldnames=asdict(my_obj).keys())
    #     writer.writeheader()
    #     writer.writerow(asdict(my_obj))
