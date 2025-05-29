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
    ParseResult,
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


DOCUMENT_RAW_FOLDER: str = "raw_downloads/documents/"
PAGE_RAW_FOLDER: str = "raw_downloads/pages/"
DOCUMENT_PROCESSED_FOLDER: str = "processed_data/documents/"
PAGE_PROCESSED_FOLDER: str = "processed_data/pages/"


def extract_urls_from_csv_file(file_name: str, sep: str = ",") -> list[URLMetadata]:
    urls: list[URLMetadata] = []
    try:
        with open(file_name, "r") as file:
            while row := file.readline():
                """
                Итерируемся по столбцам на случай, если в строке содержится не одна ссылка или ссылка содержится не в 
                первом столбце. Можно прочитать все столбцы и найти все возможные ссылки.
                
                Обрезаем " в начале и в конце строки исключительно для случая, когда файл содержит строку такого вида.
                """
                for column in row.strip("\n\"").split(sep):
                    parsed_url: ParseResult = urlparse(column)
                    if parsed_url.scheme in ("http", "https", "ftp") and parsed_url.netloc:
                        urls.append(URLMetadata(source_url=column))
    except FileNotFoundError:
        logging.warning(f"The file was not found when trying to read the URL list. File: {file_name}")
    return urls


def clear_urls_of_garbage(urls: list[URLMetadata]) -> None:
    """
    Используем регулярные выражения для поиска определенных шаблонов, чтобы не перечислять все возможные
    query-параметры, тем более когда мы можем не знать о существовании некоторых, например "burgerkingclid".
    """
    ignored_query_params_re: str = r"(^utm_|clid$|^cache_|_debug$)"
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
        logging.warning(f"Attempt to get headers failed {url}")
    else:
        """
        Попытка получить тип контента из заголовка "Content-Type".
        В случае отсутствия заголовка пытаемся узнать тип контента по URL.
        """
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
        "document": RequestsDocumentDownloader(dest_folder=DOCUMENT_RAW_FOLDER),
        "page": RequestsPageDownloader(dest_folder=PAGE_RAW_FOLDER),
    }

    """
    Скачиваем файлы и получаем нужные данные, которые может предоставить интерфейс. 
    В том числе мы получаем данные об успешности/неуспешности попытки скачивания.
    """
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
        "pdf": PDFHandler(dest_folder=DOCUMENT_PROCESSED_FOLDER),
        "docx": DocXHandler(dest_folder=DOCUMENT_PROCESSED_FOLDER),
        "xlsx": XLSXHandler(dest_folder=DOCUMENT_PROCESSED_FOLDER),
        "html": PageHandler(dest_folder=PAGE_PROCESSED_FOLDER),
    }

    """
    Обрабатываем скачанные файлы и получаем нужные данные, которые может предоставить интерфейс. 
    В том числе мы получаем данные об успешности/неуспешности попытки обработать файл.
    Для каждого типа файла мы получаем отличные от других типов файлов данные, но есть общие, которые мы можем получить
    со всех.
    """
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


def generate_csv_report(urls: list[URLMetadata], file_path: str) -> None:
    """
    Обработка случая на пустом списке нужна, так как для получения списка названий полей, который нужен для заголовка
    .csv файла, нужен экземпляр класса, т.е. любой (нулевой) элемент списка.
    Можно также получить эти поля через __annotations__ или __match_args__, но это не покрывает случай отсутствия
    аннотаций.
    """
    if len(urls) == 0:
        return

    with open(file_path, "w") as file:
        writer = csv.DictWriter(file, fieldnames=asdict(urls[0]).keys())
        writer.writeheader()
        for url in urls:
            writer.writerow(asdict(url))


if __name__ == "__main__":
    """
    Настройка логирования иных библиотек нужна исключительно для наглядности работы логирования приложения.
    """
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
