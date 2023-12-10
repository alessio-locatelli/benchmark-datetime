import datetime
from collections.abc import Callable
from functools import partial
from typing import Any

import arrow
import pendulum
import pytest
import udatetime  # type: ignore[import-untyped]
from dateutil import tz

libraries = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": pendulum.now,
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
}


@pytest.mark.parametrize("library", libraries)
def test_now_utc(benchmark: Callable[..., Any], library: str) -> None:
    func = libraries[library]
    benchmark(func)
