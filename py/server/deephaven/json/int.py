import jpy
from typing import Optional

from .type import JsonOptions

_JIntOptions = jpy.get_type("io.deephaven.json.IntOptions")


def int_(
    allow_number_int: bool = True,
    allow_number_float: bool = False,
    #allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[int] = None,
    on_missing: Optional[int] = None,
) -> JsonOptions:
    builder = (
        _JIntOptions.builder()
        .allowNumberInt(allow_number_int)
        .allowNumberFloat(allow_number_float)
        #.allowString(allow_string)
        .allowNull(allow_null)
        .allowMissing(allow_missing)
        .build()
    )
    if on_null is not None:
        builder.onNull(on_null)
    if on_missing is not None:
        builder.onMissing(on_missing)
    return JsonOptions(builder)
