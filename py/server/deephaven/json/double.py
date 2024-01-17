import jpy
from typing import Optional

from .type import JsonOptions

_JDoubleOptions = jpy.get_type("io.deephaven.json.DoubleOptions")


def double(
    allow_number: bool = True,
    allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[float] = None,
    on_missing: Optional[float] = None,
) -> JsonOptions:
    builder = (
        _JDoubleOptions.builder()
        .allowNumber(allow_number)
        .allowString(allow_string)
        .allowNull(allow_null)
        .allowMissing(allow_missing)
        .build()
    )
    if on_null is not None:
        builder.onNull(on_null)
    if on_missing is not None:
        builder.onMissing(on_missing)
    return JsonOptions(builder)
