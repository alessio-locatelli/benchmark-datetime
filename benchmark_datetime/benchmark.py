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
from dateutil.relativedelta import relativedelta
from faker import Faker
from pydantic import TypeAdapter

fake = Faker()

libraries_now_utc = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": pendulum.now,
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
    # "pydantic": ... # Not relevant.
}


@pytest.mark.parametrize("library", libraries_now_utc)
def test_now_utc(benchmark: Callable[..., Any], library: str) -> None:
    benchmark(libraries_now_utc[library])


libraries_now = {
    "arrow": arrow.now,
    # "dateutil": ..., # Not relevant.
    # "pendulum": ...,  # Not supported.
    "python": datetime.datetime.now,
    "udatetime": udatetime.now,
    # "pydantic": ... # Not relevant.
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


libraries_parse_utc_from_rfc_3339 = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,  # type: ignore[attr-defined]
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_rfc_3339)
def test_parse_utc_from_rfc_3339(benchmark: Callable[..., Any], library: str) -> None:
    rfc_3339_date_example = "1937-01-01T12:00:27.87+00:20"
    benchmark(libraries_parse_utc_from_rfc_3339[library], rfc_3339_date_example)


timedelta_kwargs = dict(
    days=+fake.pyint(min_value=400, max_value=500),
    hours=+fake.pyint(min_value=400, max_value=500),
    minutes=+fake.pyint(min_value=400, max_value=500),
    microseconds=+fake.pyint(min_value=400, max_value=500),
)
libraries_shift_forward = {
    "arrow": (lambda dt: dt.shift(**timedelta_kwargs), arrow.utcnow()),
    "dateutil": (
        lambda dt: dt + relativedelta(**timedelta_kwargs),
        datetime.datetime.now(tz.UTC),
    ),
    "pendulum": (lambda dt: dt.add(**timedelta_kwargs), pendulum.now()),
    "python": (
        lambda dt: dt + datetime.timedelta(**timedelta_kwargs),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_shift_forward)
def test_add_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    func, arg = libraries_shift_forward[library][0], libraries_shift_forward[library][1]
    benchmark(func, arg)


timedelta_kwargs_negative = dict(
    days=-fake.pyint(min_value=400, max_value=500),
    hours=-fake.pyint(min_value=400, max_value=500),
    minutes=-fake.pyint(min_value=400, max_value=500),
    microseconds=-fake.pyint(min_value=400, max_value=500),
)
libraries_shift_backward = {
    "arrow": (lambda dt: dt.shift(**timedelta_kwargs_negative), arrow.utcnow()),
    "dateutil": (
        lambda dt: dt + relativedelta(**timedelta_kwargs_negative),
        datetime.datetime.now(tz.UTC),
    ),
    "pendulum": (lambda dt: dt.add(**timedelta_kwargs_negative), pendulum.now()),
    "python": (
        lambda dt: dt + datetime.timedelta(**timedelta_kwargs_negative),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_shift_backward)
def test_substract_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    func, arg = (
        libraries_shift_backward[library][0],
        libraries_shift_backward[library][1],
    )
    benchmark(func, arg)
