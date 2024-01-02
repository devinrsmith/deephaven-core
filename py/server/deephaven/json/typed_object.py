import jpy
from typing import Dict

from .type import JsonOptions

_JTypedObjectOptions = jpy.get_type("io.deephaven.json.TypedObjectOptions")


def typed_object(
    type_field_name: str,
    shared_fields: Dict[str, JsonOptions] = {},
    objects: Dict[str, JsonOptions] = {},
    allow_unknown_types: bool = True,
    allow_null: bool = True,
    allow_missing: bool = True,
) -> JsonOptions:
    builder = (
        _JTypedObjectOptions.builder()
        .allowNull(allow_null)
        .allowMissing(allow_missing)
        .typeFieldName(type_field_name)
        .allowUnknownTypes(allow_unknown_types)
    )
    for field_name, field_opts in shared_fields.items():
        builder.putSharedFields(field_name, field_opts.j_object)
    for object_name, object_value in objects.items():
        builder.putObjects(object_name, object_value.j_object)
    return JsonOptions(builder.build())
