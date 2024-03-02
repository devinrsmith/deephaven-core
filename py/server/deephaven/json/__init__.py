from typing import Dict, List, Union, Tuple, Optional
from enum import Enum

import jpy
from datetime import datetime

from deephaven import dtypes
from deephaven.jcompat import j_hashmap, j_hashset
from deephaven.table import Table
from deephaven._wrapper import JObjectWrapper


__all__ = [
    "JsonOptions",
    "array_",
    "object_",
    "string_",
    "double_",
    "tuple_",
    "int_",
    "long_",
    "RepeatedFieldBehavior",
    "json",
    "JsonValueType",
]

_JObjectOptions = jpy.get_type("io.deephaven.json.ObjectOptions")
_JArrayOptions = jpy.get_type("io.deephaven.json.ArrayOptions")
_JTupleOptions = jpy.get_type("io.deephaven.json.TupleOptions")
_JRepeatedFieldBehavior = jpy.get_type(
    "io.deephaven.json.ObjectOptions$RepeatedFieldBehavior"
)
_JJsonValueTypes = jpy.get_type("io.deephaven.json.JsonValueTypes")
_JIntOptions = jpy.get_type("io.deephaven.json.IntOptions")
_JLongOptions = jpy.get_type("io.deephaven.json.LongOptions")
_JFloatOptions = jpy.get_type("io.deephaven.json.FloatOptions")
_JDoubleOptions = jpy.get_type("io.deephaven.json.DoubleOptions")
_JStringOptions = jpy.get_type("io.deephaven.json.StringOptions")
_JInstantOptions = jpy.get_type("io.deephaven.json.InstantOptions")
_JValueOptions = jpy.get_type("io.deephaven.json.ValueOptions")


class JsonOptions(JObjectWrapper):
    j_object_type = _JValueOptions

    def __init__(self, j_options: jpy.JType):
        self.j_options = j_options

    @property
    def j_object(self) -> jpy.JType:
        return self.j_options


class RepeatedFieldBehavior(Enum):
    ERROR = _JRepeatedFieldBehavior.ERROR
    USE_FIRST = _JRepeatedFieldBehavior.USE_FIRST


# todo use type alias instead of Any in the future
# todo named tuple
JsonValueType = Union[
    JsonOptions,
    dtypes.DType,
    type,
    Dict[str, "JsonValueType"],
    List["JsonValueType"],
    Tuple["JsonValueType", ...],
]


def _build(
    builder,
    allow_missing: bool,
    allow_null: bool,
    allow_int: bool = False,
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_bool: bool = False,
    allow_object: bool = False,
    allow_array: bool = False,
):
    builder.allowMissing(allow_missing)
    builder.desiredTypes(
        ([_JJsonValueTypes.STRING] if allow_string else [])
        + ([_JJsonValueTypes.NULL] if allow_null else [])
        + ([_JJsonValueTypes.INT] if allow_int else [])
        + ([_JJsonValueTypes.DECIMAL] if allow_decimal else [])
        + ([_JJsonValueTypes.BOOL] if allow_bool else [])
        + ([_JJsonValueTypes.OBJECT] if allow_object else [])
        + ([_JJsonValueTypes.ARRAY] if allow_array else [])
    )


def object_(
    fields: Dict[str, JsonValueType],
    allow_unknown_fields: bool = True,
    allow_null: bool = True,
    allow_missing: bool = True,
    repeated_field_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.USE_FIRST,
) -> JsonOptions:
    builder = _JObjectOptions.builder()
    _build(builder, allow_missing, allow_null, allow_object=True)
    builder.repeatedFieldBehavior(repeated_field_behavior.value)
    builder.allowUnknownFields(allow_unknown_fields)
    for field_name, field_opts in fields.items():
        builder.putFields(field_name, json(field_opts).j_options)
    return JsonOptions(builder.build())


def array_(
    element: JsonValueType,
    allow_null: bool = True,
    allow_missing: bool = True,
) -> JsonOptions:
    builder = _JArrayOptions.builder()
    builder.element(json(element).j_options)
    _build(builder, allow_missing, allow_null, allow_array=True)
    return JsonOptions(builder.build())


def tuple_(values: Tuple[JsonValueType, ...]) -> JsonOptions:
    return JsonOptions(_JTupleOptions.of([json(opt).j_options for opt in values]))


def int_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[int] = None,
    on_missing: Optional[int] = None,
) -> JsonOptions:
    builder = _JIntOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def long_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[int] = None,
    on_missing: Optional[int] = None,
) -> JsonOptions:
    builder = _JLongOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def float_(
    allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[float] = None,
    on_missing: Optional[float] = None,
) -> JsonOptions:
    builder = _JFloatOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def double_(
    allow_string: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[float] = None,
    on_missing: Optional[float] = None,
) -> JsonOptions:
    builder = _JDoubleOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def string_(
    allow_int: bool = False,
    allow_decimal: bool = False,
    allow_bool: bool = False,
    allow_null: bool = True,
    allow_missing: bool = True,
    on_null: Optional[str] = None,
    on_missing: Optional[str] = None,
) -> JsonOptions:
    builder = _JStringOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_string=True,
        allow_int=allow_int,
        allow_decimal=allow_decimal,
        allow_bool=allow_bool,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def instant_(
    allow_null: bool = True,
    allow_missing: bool = True,
    # todo on_null, on_missing
) -> JsonOptions:
    builder = _JInstantOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_string=True,
    )
    return JsonOptions(builder.build())


# TODO: encode optional?
_dtype_dict = {
    dtypes.int32: int_(),
    dtypes.int64: long_(),
    dtypes.float32: float_(),
    dtypes.float64: double_(),
    dtypes.string: string_(),
    dtypes.Instant: instant_(),
}

_type_dict = {
    int: long_(),
    float: double_(),
    str: string_(),
    datetime: instant_(),
}


def json(json_value_type: JsonValueType) -> JsonOptions:
    if isinstance(json_value_type, JsonOptions):
        return json_value_type
    if isinstance(json_value_type, dtypes.DType):
        return _dtype_dict[json_value_type]
    if isinstance(json_value_type, type):
        return _type_dict[json_value_type]
    if isinstance(json_value_type, Dict):
        return object_(json_value_type)
    if isinstance(json_value_type, List):
        if len(json_value_type) is not 1:
            raise TypeError("Expected List as json type to have exactly one element")
        return array_(json_value_type[0])
    if isinstance(json_value_type, Tuple):
        return tuple_(json_value_type)
    raise TypeError("unexpected")
