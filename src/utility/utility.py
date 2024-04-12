import time
from requests import Response
from utility.variables import SPOTIFY_RATE_LIMIT_RESPONSE_CODE
from typing import Any, Callable


def is_success_code(code: int) -> bool:
    return 200 <= code < 300


def send_request_with_wait(request_func: Callable[..., Response], *args: Any) -> Response:
    """
    This function computes the callable request and evaluates the response. If the response
    code is a rate limit response, sleep until the rate limit is no longer applied.

    Args:
        request_func (Callable[[], Response]): _description_

    Returns:
        Response: _description_
    """
    res = request_func(*args)
    while (res.status_code == SPOTIFY_RATE_LIMIT_RESPONSE_CODE):
        print(f"INFO: Exceeded rate limit, sleeping for {res.headers['Retry-After']} seconds")
        time.sleep(int(res.headers['Retry-After']))
        res = request_func(*args)
    return res
