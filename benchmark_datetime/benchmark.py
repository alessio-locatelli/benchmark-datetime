import datetime
from collections.abc import Callable
from functools import partial
from typing import Any

import arrow
import pendulum
import pydantic
import pytest
import udatetime  # type: ignore[import-untyped]
from dateutil import tz
from faker import Faker
from pydantic import TypeAdapter

fake = Faker()

libraries_now_utc = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": pendulum.now,
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
    # "pydantic": ... # Not supported.
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
    # "pydantic": ... # Not supported.
}


@pytest.mark.parametrize("library", libraries_now)
def test_now(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(libraries_now[library])


libraries_parse_utc_from_unix_timestamp = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.from_timestamp,
    "python": datetime.datetime.utcfromtimestamp,
    "udatetime": udatetime.utcfromtimestamp,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_unix_timestamp)
def test_parse_utc_from_timestamp(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(libraries_parse_utc_from_unix_timestamp[library], fake.unix_time())


libraries_parse_utc_from_iso_8601 = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,  # type: ignore[attr-defined]
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_iso_8601)
def test_parse_utc_from_iso_8601(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(
        libraries_parse_utc_from_iso_8601[library],
        fake.iso8601(tzinfo=datetime.UTC),
    )
