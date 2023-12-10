import datetime
from collections.abc import Callable
from functools import partial
from typing import Any

import arrow
import pendulum
import pytest
import udatetime  # type: ignore[import-untyped]
from dateutil import tz

libraries_now_utc = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": pendulum.now,
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
}


@pytest.mark.parametrize("library", libraries_now_utc)
def test_now_utc(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(libraries_now_utc[library])


libraries_now = {
    "arrow": arrow.now,
    "dateutil": datetime.datetime.now,
    # "pendulum": ...,  # Not supported.
    "python": datetime.datetime.now,
    "udatetime": udatetime.now,
}


@pytest.mark.parametrize("library", libraries_now)
def test_now(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(libraries_now_utc[library])
