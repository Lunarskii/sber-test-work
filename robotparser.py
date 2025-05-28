from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from urllib.error import URLError


_robots_cache: dict[str, RobotFileParser] = {}


def can_fetch(url: str, user_agent: str) -> bool:
    parsed_url = urlparse(url)
    sitemap: str = f"{parsed_url.scheme}://{parsed_url.netloc}"
    robot_parser: RobotFileParser | None = _robots_cache.get(sitemap)

    if not robot_parser:
        robots_url: str = sitemap + "/robots.txt"
        robot_parser = RobotFileParser()
        robot_parser.set_url(robots_url)

        try:
            robot_parser.read()
        except URLError:
            robot_parser = None
        _robots_cache[sitemap] = robot_parser
    if robot_parser is None:
        return True

    return robot_parser.can_fetch(url=url, useragent=user_agent)
