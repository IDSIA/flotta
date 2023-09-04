from requests import Session
from requests.adapters import HTTPAdapter, Retry


def check_url(url: str, total_retry=10, backoff_factor=0.5) -> None:
    """Checks that the given url up, if not wait for a little bit.
    If the url cannot be accessed, or it returns 500 error, raises
    an exception; otherwise nothing happens.
    """
    s = Session()

    retries = Retry(
        total=total_retry,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
    )

    s.mount("http://", HTTPAdapter(max_retries=retries))

    res = s.get(url)

    res.raise_for_status()
