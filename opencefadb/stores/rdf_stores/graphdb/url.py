def _parse_url(url):
    if url == "localhost":
        return "http://localhost"
    if url.endswith("/"):
        return url[:-1]
    return url