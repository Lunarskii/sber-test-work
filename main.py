import csv
import re
import argparse
import mimetypes
import logging
from dataclasses import asdict

from urllib.parse import (
    urlparse,
    parse_qsl,
    urlunparse,
    urlencode,
)
import requests
from requests.exceptions import RequestException

from downloaders import (
    ContentDownloader,
    RequestsDocumentDownloader,
    RequestsPageDownloader,
)
from handlers import (
    ContentHandler,
    PDFHandler,
    DocXHandler,
    XLSXHandler,
    PageHandler,
)
from schemas import URLMetadata
from logger import configure_logging


def extract_urls_from_csv_file(file_name: str) -> list[URLMetadata]:
    urls: list[URLMetadata] = []
    with open(file_name, "r") as file:
        csv_reader = csv.reader(file, doublequote=False)
        for row in csv_reader:
            for column in row:
                parsed_url = urlparse(column)
                if parsed_url.scheme in ("http", "https", "ftp") and parsed_url.netloc:
                    urls.append(URLMetadata(source_url=column))
    return urls


def clear_urls_of_garbage(urls: list[URLMetadata]) -> None:
    ignored_query_params_re: str = r"(^utm_|clid$|^cache_)"
    pattern: re.Pattern = re.compile(ignored_query_params_re)

    for url in urls:
        parsed_url = urlparse(url.source_url)
        query_params = [(key, value) for key, value in parse_qsl(parsed_url.query) if not pattern.search(key)]
        new_parsed_url = parsed_url._replace(query=urlencode(query_params, doseq=True))
        url.source_url = str(urlunparse(new_parsed_url))


def get_content_type(url: str) -> tuple[str, str]:
    try:
        response: requests.Response = requests.head(url, allow_redirects=True)
    except RequestException:
        ...
    else:
        content_type = response.headers.get("Content-Type")

        if not content_type:
            content_type = mimetypes.guess_type(url)[0]
        else:
            content_type = content_type.split(";")[0].strip().lower()
        ext_type = mimetypes.guess_extension(content_type)

        if ext_type:
            return "page" if ext_type == ".html" else "document", ext_type
    return "", ""


def download_files(urls: list[URLMetadata]) -> None:
    downloaders: dict[str, ContentDownloader] = {
        "document": RequestsDocumentDownloader(dest_folder="raw_downloads/documents/"),
        "page": RequestsPageDownloader(dest_folder="raw_downloads/pages/"),
    }

    for url in urls:
        content_type, ext_type = get_content_type(url.source_url)

        if content_type not in downloaders.keys():
            url.download_status = "failed_download"
        else:
            file_name: str = url.id + ext_type
            current_downloader: ContentDownloader = downloaders.get(content_type)

            url.content_type_detected = content_type
            current_downloader.download(url.source_url, file_name=file_name)
            url.final_url = current_downloader.url
            url.download_status = current_downloader.download_status
            url.error_message = current_downloader.error_message
            url.raw_file_path = current_downloader.file_path
            url.file_size_bytes = current_downloader.file_size_bytes


def handle_files(urls: list[URLMetadata]) -> None:
    handlers: dict[str, ContentHandler] = {
        "pdf": PDFHandler(dest_folder="processed_data/documents/"),
        "docx": DocXHandler(dest_folder="processed_data/documents/"),
        "xlsx": XLSXHandler(dest_folder="processed_data/documents/"),
        "html": PageHandler(dest_folder="processed_data/pages/"),
    }

    for url in urls:
        if url.download_status == "failed_download":
            continue

        ext_type: str = url.raw_file_path.split("/")[-1].split(".")[-1]

        if ext_type not in handlers.keys():
            url.download_status = "failed_processing"
        else:
            current_handler: ContentHandler = handlers.get(ext_type)

            current_handler.handle(url.raw_file_path)
            url.download_status = current_handler.download_status
            url.error_message = current_handler.error_message
            url.processed_file_path = current_handler.path
            url.detected_language = current_handler.metadata.get("language")
            if ext_type in ("pdf", "docx"):
                url.document_page_count = current_handler.metadata.get("document_page_count")
                url.metadata_author = current_handler.metadata.get("author")
                url.metadata_creation_date = current_handler.metadata.get("creation_date")
            elif ext_type == "xlsx":
                url.metadata_author = current_handler.metadata.get("author")
                url.metadata_creation_date = current_handler.metadata.get("creation_date")
            else:
                ...


def generate_csv_report(urls: list[URLMetadata], file_path: str) -> None:
    if len(urls) == 0:
        return

    with open(file_path, "w") as file:
        writer = csv.DictWriter(file, fieldnames=asdict(urls[0]).keys())
        writer.writeheader()
        for url in urls:
            writer.writerow(asdict(url))


if __name__ == "__main__":
    configure_logging(level=logging.DEBUG, log_file="app.log")
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.getLogger("pypdf").setLevel(logging.CRITICAL)

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file")
    args: argparse.Namespace = parser.parse_args()

    urls: list[URLMetadata] = extract_urls_from_csv_file(args.csv_file)
    clear_urls_of_garbage(urls)

    download_files(urls)
    handle_files(urls)

    generate_csv_report(urls, "results_registry.csv")
