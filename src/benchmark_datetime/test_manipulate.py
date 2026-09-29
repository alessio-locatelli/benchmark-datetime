import datetime
from functools import partial
from typing import TYPE_CHECKING, Any

import arrow
import pendulum
import pytest
import udatetime
from dateutil import tz
from dateutil.relativedelta import SA, relativedelta
from faker import Faker
from whenever import TimeDelta, ZonedDateTime

if TYPE_CHECKING:
    from collections.abc import Callable

fake = Faker()
Faker.seed(0)

EQUIVALENCE_TOLERANCE_SECONDS = 2.0


def _epoch_seconds(
    dt: datetime.datetime | arrow.Arrow | pendulum.DateTime | ZonedDateTime,
) -> float:
    if isinstance(dt, ZonedDateTime):
        return dt.timestamp_millis() / 1000
    return dt.timestamp()


libraries_now_utc = {
    "arrow": arrow.utcnow,
    "dateutil": partial(datetime.datetime.now, tz.UTC),
    "pendulum": partial(pendulum.now, pendulum.UTC),
    "python": partial(datetime.datetime.now, datetime.UTC),
    "udatetime": udatetime.utcnow,
    # "pydantic": ... # Not relevant.
    "whenever": partial(ZonedDateTime.now, "UTC"),
}


@pytest.mark.parametrize("library", libraries_now_utc)
def test_now_utc(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = [_epoch_seconds(func()) for func in libraries_now_utc.values()]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

    benchmark(libraries_now_utc[library])


libraries_now_local = {
    "arrow": arrow.now,
    # "dateutil": ..., # Not relevant.
    "pendulum": pendulum.now,
    "python": datetime.datetime.now,
    "udatetime": udatetime.now,
    # "pydantic": ... # Not relevant.
    "whenever": ZonedDateTime.now_in_system_tz,
}


@pytest.mark.parametrize("library", libraries_now_local)
def test_now_local(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = [_epoch_seconds(func()) for func in libraries_now_local.values()]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

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
        lambda dt: dt + relativedelta(**timedelta_kwargs),  # type: ignore[arg-type]
        datetime.datetime.now(tz.UTC),
    ),
    "pendulum": (lambda dt: dt.add(**timedelta_kwargs), pendulum.now(pendulum.UTC)),
    "python": (
        lambda dt: dt + datetime.timedelta(**timedelta_kwargs),
        datetime.datetime.now(datetime.UTC),
    ),
    # "udatetime": ...,  # Not relevant.
    # "pydantic": ...,  # Not relevant.
    "whenever": (lambda dt: dt.add(**timedelta_kwargs), ZonedDateTime.now("UTC")),
}


@pytest.mark.parametrize("library", libraries_shift_forward)
def test_add_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = [
        _epoch_seconds(func(arg)) for func, arg in libraries_shift_forward.values()
    ]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

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
        lambda dt: dt + relativedelta(**timedelta_kwargs_negative),  # type: ignore[arg-type]
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
    "whenever": (
        lambda dt: dt.add(**timedelta_kwargs_negative),
        ZonedDateTime.now("UTC"),
    ),
}


@pytest.mark.parametrize("library", libraries_shift_backward)
def test_substract_timedelta(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = {
        k: _epoch_seconds(v[0](v[1])) for k, v in libraries_shift_backward.items()
    }
    assert (
        max(timestamps.values()) - min(timestamps.values())
        < EQUIVALENCE_TOLERANCE_SECONDS
    ), timestamps

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
    "whenever": (
        lambda td: td.total("seconds"),
        TimeDelta(days_assumed_24h_ok=True, **timedelta_kwargs),
    ),
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


libraries_weekday = {
    "arrow": (lambda dt: dt.weekday(), arrow.utcnow()),
    # "dateutil": ...,  # Not supported.
    "pendulum": (lambda td: td.day_of_week, pendulum.now(pendulum.UTC)),
    "python": (lambda td: td.weekday(), datetime.datetime.now(datetime.UTC)),
    "udatetime": (lambda td: td.weekday(), udatetime.utcnow()),
    # "pydantic": ...,  # Not relevant.
    "whenever": (
        lambda dt: dt.date().day_of_week().value - 1,
        ZonedDateTime.now("UTC"),
    ),
}


@pytest.mark.parametrize("library", libraries_weekday)
def test_weekday(benchmark: Callable[..., Any], library: str) -> None:
    # Functions from different libraries give the same result.
    assert len({func(arg) for func, arg in libraries_weekday.values()}) == 1

    func, arg = (libraries_weekday[library][0], libraries_weekday[library][1])
    benchmark(func, arg)


SATURDAY = 5
FIND_NEXT_SATURDAY_REFERENCE_TUESDAY = (2024, 1, 2, 12, 0, 0)
libraries_find_next_saturday = {
    "arrow": (
        lambda dt: dt.shift(weekday=SATURDAY),
        arrow.Arrow(*FIND_NEXT_SATURDAY_REFERENCE_TUESDAY),
    ),
    "dateutil": (
        lambda dt: dt + relativedelta(weekday=SA(+1)),
        datetime.datetime(*FIND_NEXT_SATURDAY_REFERENCE_TUESDAY, tzinfo=tz.UTC),
    ),
    "pendulum": (
        lambda dt: (
            dt
            if dt.day_of_week == SATURDAY
            else dt.next(pendulum.SATURDAY, keep_time=True)
        ),
        pendulum.datetime(*FIND_NEXT_SATURDAY_REFERENCE_TUESDAY, tz="UTC"),
    ),
    "python": (
        lambda dt: dt + datetime.timedelta((7 + SATURDAY - dt.weekday()) % 7),
        datetime.datetime(*FIND_NEXT_SATURDAY_REFERENCE_TUESDAY, tzinfo=datetime.UTC),
    ),
    # "udatetime": ..., # Not relevant
    # "pydantic": ...,  # Not relevant.
    "whenever": (
        lambda dt: (
            dt
            if dt.date().day_of_week().value - 1 == SATURDAY
            else dt.add(days=(7 + SATURDAY - (dt.date().day_of_week().value - 1)) % 7)
        ),
        ZonedDateTime(*FIND_NEXT_SATURDAY_REFERENCE_TUESDAY, tz="UTC"),
    ),
}


@pytest.mark.parametrize("library", libraries_find_next_saturday)
def test_find_next_saturday(benchmark: Callable[..., Any], library: str) -> None:
    timestamps = [
        _epoch_seconds(func(arg)) for func, arg in libraries_find_next_saturday.values()
    ]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, timestamps

    func, arg = (
        libraries_find_next_saturday[library][0],
        libraries_find_next_saturday[library][1],
    )
    benchmark(func, arg)
