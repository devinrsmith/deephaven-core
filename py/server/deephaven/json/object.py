from typing import Dict, Any
from enum import Enum
import jpy

from .type import JsonOptions

_JObjectOptions = jpy.get_type("io.deephaven.json.ObjectOptions")
_JRepeatedFieldBehavior = jpy.get_type(
    "io.deephaven.json.ObjectOptions$RepeatedFieldBehavior"
)


class RepeatedFieldBehavior(Enum):
    ERROR = _JRepeatedFieldBehavior.ERROR
    USE_FIRST = _JRepeatedFieldBehavior.USE_FIRST


def object_(
    fields: Dict[str, JsonOptions],
    allow_unknown_fields: bool = True,
    allow_null: bool = True,
    allow_missing: bool = True,
    repeated_field_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.USE_FIRST,
) -> JsonOptions:
    builder = (
        _JObjectOptions.builder()
        .allowUnknownFields(allow_unknown_fields)
        .allowNull(allow_null)
        .allowMissing(allow_missing)
        .repeatedFieldBehavior(repeated_field_behavior.value)
    )
    for field_name, field_opts in fields.items():
        builder.putFields(field_name, field_opts.j_object)
    return JsonOptions(builder.build())
