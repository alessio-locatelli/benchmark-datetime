import datetime
from collections.abc import Callable
from typing import Any

import arrow
import dateutil
import pandas as pd
import pendulum
import pydantic
import pytest
import udatetime  # type: ignore[import-untyped]
from faker import Faker
from pydantic import TypeAdapter

fake = Faker()


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
    # Functions from different libraries give the same result.
    unix_time = fake.unix_time()
    datetimes = [
        func(unix_time) for func in libraries_parse_utc_from_unix_timestamp.values()
    ]
    assert len({dt.day for dt in datetimes}) == 1

    benchmark(libraries_parse_utc_from_unix_timestamp[library], unix_time)


libraries_parse_utc_from_iso_8601 = {
    "arrow": arrow.get,
    "dateutil": dateutil.parser.isoparse,
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
    "pandas": lambda dt: pd.Timestamp(dt).to_pydatetime(),
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_iso_8601)
def test_parse_utc_from_iso_8601(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    fake_iso8601 = fake.iso8601(tzinfo=datetime.UTC)
    datetimes = [
        func(fake_iso8601) for func in libraries_parse_utc_from_iso_8601.values()
    ]
    assert len({dt.day for dt in datetimes}) == 1

    benchmark(libraries_parse_utc_from_iso_8601[library], fake_iso8601)


libraries_parse_utc_from_iso_8601_duration = {
    # "arrow": ...,  # Not supported (https://github.com/arrow-py/arrow/issues/757).
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,
    # "python": ...,  # Not supported.
    # "udatetime": ...,  # Not supported.
    "pydantic": TypeAdapter(datetime.timedelta).validate_python,
    "pandas": pd.Timedelta,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_iso_8601_duration)
def test_parse_utc_from_iso_8601_duration(
    benchmark: Callable[..., Any],
    library: str,
) -> None:
    # Functions from different libraries give the same result.
    iso8601_examples = [
        # "P3Y6M4DT12H30M5S",  # Raises "ValueError: Invalid ISO 8601 Duration format"
        # "P4Y",  # Raises "ValueError: Invalid ISO 8601 Duration format"
        "P1DT12H",
    ]
    durations = [
        func(iso8601_examples[0])  # type: ignore[operator]
        for func in libraries_parse_utc_from_iso_8601_duration.values()
    ]
    assert len({dt.total_seconds() for dt in durations}) == 1

    benchmark(libraries_parse_utc_from_iso_8601_duration[library], iso8601_examples[0])


libraries_parse_utc_from_rfc_3339 = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
    "pandas": lambda dt: pd.Timestamp(dt).to_pydatetime(),
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_rfc_3339)
def test_parse_utc_from_rfc_3339(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    datetimes = [
        func("1996-12-19T16:39:57-08:00")
        for func in libraries_parse_utc_from_rfc_3339.values()
    ]
    assert len({dt.day for dt in datetimes}) == 1

    func = libraries_parse_utc_from_rfc_3339[library]

    def parse() -> None:
        for rfc_3339_datetime in [
            # Examples are taken from https://datatracker.ietf.org/doc/html/rfc3339.
            "1985-04-12T23:20:50.52Z",
            "1996-12-19T16:39:57-08:00",
            # "1990-12-31T23:59:60Z",  # Can not be parsed out of the box.
            # "1990-12-31T15:59:60-08:00",  # Can not be parsed out of the box.
            "1937-01-01T12:00:27.87+00:20",
        ]:
            func(rfc_3339_datetime)

    benchmark(parse)
