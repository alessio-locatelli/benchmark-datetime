import datetime
from collections.abc import Callable
from typing import Any

import arrow
import pendulum
import pytest
import udatetime  # type: ignore[import-untyped]
from faker import Faker

fake = Faker()


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
