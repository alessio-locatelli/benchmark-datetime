import datetime
from typing import TYPE_CHECKING, Any

import arrow
import pendulum
import pytest
import udatetime
from faker import Faker
from whenever import OffsetDateTime

if TYPE_CHECKING:
    from collections.abc import Callable

fake = Faker()
Faker.seed(0)

EQUIVALENCE_TOLERANCE_SECONDS = 2.0


libraries_convert_dt_to_isoformat_string = {
    "arrow": (lambda dt: dt.isoformat(), arrow.utcnow()),
    # "dateutil": ...,  # Not relevant.
    "pendulum": (lambda dt: dt.isoformat(), pendulum.now(pendulum.UTC)),
    "python": (lambda dt: dt.isoformat(), datetime.datetime.now(datetime.UTC)),
    "udatetime": (udatetime.to_string, udatetime.utcnow()),
    # "pydantic": ...,  # Not relevant.
    "whenever": (
        lambda dt: dt.format_iso(unit="microsecond"),
        OffsetDateTime.now(0, stale_offset_ok=True),
    ),
}


@pytest.mark.parametrize("library", libraries_convert_dt_to_isoformat_string)
def test_convert_dt_to_isoformat_string(
    benchmark: Callable[..., Any],
    library: str,
) -> None:
    iso_strings = [
        func(arg) for func, arg in libraries_convert_dt_to_isoformat_string.values()
    ]
    timestamps = [datetime.datetime.fromisoformat(s).timestamp() for s in iso_strings]
    assert max(timestamps) - min(timestamps) < EQUIVALENCE_TOLERANCE_SECONDS, (
        iso_strings
    )

    func, arg = (
        libraries_convert_dt_to_isoformat_string[library][0],
        libraries_convert_dt_to_isoformat_string[library][1],
    )
    benchmark(func, arg)
