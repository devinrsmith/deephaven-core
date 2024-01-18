from typing import Optional
import jpy

from .type import JsonOptions

_JStringOptions = jpy.get_type("io.deephaven.json.StringOptions")


def string(
    allow_string: bool = True,
    allow_number_int: bool = False,
    allow_number_float: bool = False,
    allow_boolean: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[str] = None,
    on_missing: Optional[str] = None,
) -> JsonOptions:
    builder = (
        _JStringOptions.builder()
        .allowString(allow_string)
        .allowNumberInt(allow_number_int)
        .allowNumberFloat(allow_number_float)
        .allowBoolean(allow_boolean)
        .allowNull(allow_null)
        .allowMissing(allow_missing)
        .build()
    )
    if on_null is not None:
        builder.onNull(on_null)
    if on_missing is not None:
        builder.onMissing(on_missing)
    return JsonOptions(builder)
