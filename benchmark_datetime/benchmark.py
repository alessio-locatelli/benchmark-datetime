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
from dateutil.relativedelta import SA, relativedelta
from faker import Faker
from pydantic import TypeAdapter

fake = Faker()

libraries_now_utc = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": partial(pendulum.now, pendulum.UTC),
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
    # "pydantic": ... # Not relevant.
}


@pytest.mark.parametrize("library", libraries_now_utc)
def test_now_utc(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    datetimes = [func() for func in libraries_now_utc.values()]
    assert len({dt.hour for dt in datetimes}) == 1

    benchmark(libraries_now_utc[library])


libraries_now_local = {
    "arrow": arrow.now,
    # "dateutil": ..., # Not relevant.
    "pendulum": pendulum.now,
    "python": datetime.datetime.now,
    "udatetime": udatetime.now,
    # "pydantic": ... # Not relevant.
}


@pytest.mark.parametrize("library", libraries_now_local)
def test_now_local(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    datetimes = [func() for func in libraries_now_local.values()]
    assert len({dt.hour for dt in datetimes}) == 1

    benchmark(libraries_now_local[library])


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
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
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


libraries_parse_utc_from_rfc_3339 = {
    "arrow": arrow.get,
    # "dateutil": ...,  # Not supported.
    "pendulum": pendulum.parse,
    "python": datetime.datetime.fromisoformat,
    "udatetime": udatetime.from_string,
    "pydantic": TypeAdapter(pydantic.AwareDatetime).validate_python,
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
    "pendulum": (lambda dt: dt.add(**timedelta_kwargs), pendulum.now(pendulum.UTC)),
    "python": (
        lambda dt: dt + datetime.timedelta(**timedelta_kwargs),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_shift_forward)
def test_add_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    datetimes = [func(arg) for func, arg in libraries_shift_forward.values()]
    assert len({dt.day for dt in datetimes}) == 1, datetimes

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
    "pendulum": (
        lambda dt: dt.add(**timedelta_kwargs_negative),
        pendulum.now(pendulum.UTC),
    ),
    "python": (
        lambda dt: dt + datetime.timedelta(**timedelta_kwargs_negative),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_shift_backward)
def test_substract_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    providers_datetimes = {k: v[0](v[1]) for k, v in libraries_shift_forward.items()}
    assert (
        len({dt.day for dt in providers_datetimes.values()}) == 1
    ), providers_datetimes

    func, arg = (
        libraries_shift_backward[library][0],
        libraries_shift_backward[library][1],
    )
    benchmark(func, arg)


libraries_timedelta_to_seconds = {
    # "arrow": ...,  # Not relevant.
    # "dateutil": ...,  # Not relevant.
    "pendulum": (lambda td: td.total_seconds(), pendulum.duration(**timedelta_kwargs)),
    "python": (lambda td: td.total_seconds(), datetime.timedelta(**timedelta_kwargs)),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_timedelta_to_seconds)
def test_timedelta_to_seconds(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    assert (
        len({func(arg) for func, arg in libraries_timedelta_to_seconds.values()}) == 1
    )

    func, arg = (
        libraries_timedelta_to_seconds[library][0],
        libraries_timedelta_to_seconds[library][1],
    )
    benchmark(func, arg)


libraries_isoweekday = {
    "arrow": (lambda dt: dt.isoweekday(), arrow.utcnow()),
    # "dateutil": ...,  # Not supported.
    "pendulum": (lambda td: td.day_of_week, pendulum.now(pendulum.UTC)),
    "python": (lambda td: td.isoweekday(), datetime.datetime.now(datetime.UTC)),
    "udatetime": (lambda td: td.isoweekday(), udatetime.utcnow()),
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_isoweekday)
def test_isoweekday(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    assert len({func(arg) for func, arg in libraries_isoweekday.values()}) == 1

    func, arg = (libraries_isoweekday[library][0], libraries_isoweekday[library][1])
    benchmark(func, arg)


SATURDAY = 5
libraries_find_next_saturday = {
    "arrow": (lambda dt: dt.shift(weekday=SATURDAY), arrow.utcnow()),
    "dateutil": (
        lambda dt: dt + relativedelta(weekday=SA(+1)),
        datetime.datetime.now(tz.UTC),
    ),
    "pendulum": (lambda dt: dt.next(pendulum.SATURDAY), pendulum.now(pendulum.UTC)),
    "python": (
        lambda dt: dt + datetime.timedelta((7 + SATURDAY - dt.weekday()) % 7),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ..., # Not relevant
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_find_next_saturday)
def test_find_next_saturday(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    datetimes = [func(arg) for func, arg in libraries_find_next_saturday.values()]
    assert len({dt.day for dt in datetimes}) == 1

    func, arg = (
        libraries_find_next_saturday[library][0],
        libraries_find_next_saturday[library][1],
    )
    benchmark(func, arg)


libraries_convert_dt_to_isoformat_string = {
    "arrow": (lambda dt: dt.isoformat(), arrow.utcnow()),
    # "dateutil": ...,  # Not relevant.
    "pendulum": (lambda dt: dt.isoformat(), pendulum.now(pendulum.UTC)),
    "python": (lambda dt: dt.isoformat(), datetime.datetime.now(datetime.UTC)),
    "udatetime": (udatetime.to_string, udatetime.utcnow()),
    # "pydantic": ...,  # Not relevant.
}


@pytest.mark.parametrize("library", libraries_convert_dt_to_isoformat_string)
def test_convert_dt_to_isoformat_string(
    benchmark: Callable[..., Any],
    library: str,
) -> None:
    # Functions from different libraries give the same result.
    iso_strings = [
        func(arg) for func, arg in libraries_convert_dt_to_isoformat_string.values()
    ]
    assert len({len(s) for s in iso_strings}) == 1, {s: len(s) for s in iso_strings}

    func, arg = (
        libraries_convert_dt_to_isoformat_string[library][0],
        libraries_convert_dt_to_isoformat_string[library][1],
    )
    benchmark(func, arg)
