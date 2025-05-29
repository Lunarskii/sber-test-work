from logging import (
    INFO,
    Logger,
    getLogger,
    Formatter,
    StreamHandler,
    FileHandler,
)


def configure_logging(
    level: str | int = INFO,
    log_file: str = None,
    logger: Logger = None,
) -> None:
    if isinstance(level, str):
        level = level.upper()
    root = logger or getLogger()
    root.setLevel(level)

    fmt = Formatter(fmt="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console = StreamHandler()
    console.setLevel(level)
    console.setFormatter(fmt)
    root.addHandler(console)

    if log_file:
        file_handler = FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
