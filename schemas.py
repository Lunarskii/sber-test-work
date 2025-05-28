import uuid

from dataclasses import (
    dataclass,
    field,
)
from datetime import datetime
from typing import Literal


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
@dataclass(kw_only=True)
class URLMetadata:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_url: str = field(default=None)
    final_url: str = field(default=None)
    download_timestamp: str = field(default=None)
    download_status: Literal["success", "failed_download", "failed_processing", "skipped_robots"] = field(default="success")
    error_message: str = field(default=None)
    content_type_detected: Literal["document", "page"] = field(default=None)
    raw_file_path: str = field(default=None)
    processed_file_path: str = field(default=None)
    file_size_bytes: int = field(default=None)
    document_page_count: int = field(default=None)
    detected_language: str = field(default=None)
    extracted_keywords: list[str] = field(default=None)
    extracted_entities: list[str] = field(default=None)
    summary: str = field(default=None)
    metadata_author: str = field(default=None)
    metadata_creation_date: str = field(default=None)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == "raw_file_path" and value:
            time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            super().__setattr__("download_timestamp", time)
