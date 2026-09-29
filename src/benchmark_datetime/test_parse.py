import datetime
from functools import partial
from typing import TYPE_CHECKING, Any

import arrow
import dateutil
import pandas as pd
import pendulum
import pydantic
import pytest
import udatetime
from faker import Faker
from pydantic import TypeAdapter
from whenever import Instant, ItemizedDelta, OffsetDateTime, TimeDelta

if TYPE_CHECKING:
    from collections.abc import Callable

fake = Faker()
Faker.seed(0)

EQUIVALENCE_TOLERANCE_SECONDS = 0.01


def _epoch_seconds(
    dt: datetime.datetime | arrow.Arrow | pendulum.DateTime | Instant | OffsetDateTime,
) -> float:
    if isinstance(dt, Instant | OffsetDateTime):
        return dt.timestamp_millis() / 1000
    return dt.timestamp()


libraries_parse_utc_from_unix_timestamp = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.from_timestamp,
    "python": partial(datetime.datetime.fromtimestamp, tz=datetime.UTC),
    "udatetime": udatetime.utcfromtimestamp,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
    "whenever": Instant.from_timestamp,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_unix_timestamp)
def test_parse_utc_from_timestamp(benchmark: Callable[..., Any], library: str) -> None:
    unix_time = fake.unix_time()
    timestamps = [
        _epoch_seconds(func(unix_time))
        for func in libraries_parse_utc_from_unix_timestamp.values()
    ]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

    benchmark(libraries_parse_utc_from_unix_timestamp[library], unix_time)


libraries_parse_utc_from_iso_8601 = {
    "arrow": arrow.get,
    "dateutil": dateutil.parser.isoparse,
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
    "pandas": lambda dt: pd.Timestamp(dt).to_pydatetime(),
    "whenever": OffsetDateTime.parse_iso,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_iso_8601)
def test_parse_utc_from_iso_8601(benchmark: Callable[..., Any], library: str) -> None:
    fake_iso8601 = fake.iso8601(tzinfo=datetime.UTC)
    timestamps = [
        _epoch_seconds(func(fake_iso8601))
        for func in libraries_parse_utc_from_iso_8601.values()
    ]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

    benchmark(libraries_parse_utc_from_iso_8601[library], fake_iso8601)


libraries_parse_utc_from_iso_8601_duration = {
    # "arrow": ...,  # Not supported (https://github.com/arrow-py/arrow/issues/757).
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,
    # "python": ...,  # Not supported.
    # "udatetime": ...,  # Not supported.
    "pydantic": TypeAdapter(datetime.timedelta).validate_python,
    "pandas": pd.Timedelta,
    "whenever": lambda s: TimeDelta(
        days_assumed_24h_ok=True,
        **dict(ItemizedDelta.parse_iso(s)),
    ).to_stdlib(),
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
    "dateutil": dateutil.parser.isoparse,
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
    "pandas": lambda dt: pd.Timestamp(dt).to_pydatetime(),
    "whenever": OffsetDateTime.parse_iso,
}


@pytest.mark.parametrize("library", libraries_parse_utc_from_rfc_3339)
def test_parse_utc_from_rfc_3339(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = [
        _epoch_seconds(func("1996-12-19T16:39:57-08:00"))
        for func in libraries_parse_utc_from_rfc_3339.values()
    ]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

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
