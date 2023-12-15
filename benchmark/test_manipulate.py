import datetime
from collections.abc import Callable
from functools import partial
from typing import Any

import arrow
import pendulum
import pytest
import udatetime  # type: ignore[import-untyped]
from dateutil import tz
from dateutil.relativedelta import SA, relativedelta
from faker import Faker

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
