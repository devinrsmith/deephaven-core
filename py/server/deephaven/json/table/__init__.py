#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy
from pathlib import Path
from typing import Optional, Union, List

from deephaven.json import json, JsonValueType
from deephaven.table import Table

__all__ = ["source", "Json_table"]

_JJsonTableOptions = jpy.get_type("io.deephaven.json.JsonTableOptions")
_JSource = jpy.get_type("io.deephaven.json.Source")
_JFile = jpy.get_type("java.io.File")
_JUrl = jpy.get_type("java.net.URL")
_JByteBuffer = jpy.get_type("java.nio.ByteBuffer")
_JTableProcessorOptions = jpy.get_type(
    "io.deephaven.engine.table.impl.processor.TableProcessorOptions"
)

# os.PathLike?


def source(
    content: Union[None, str, bytes] = None,
    file: Union[None, str, Path] = None,
    url: Optional[str] = None,
):
    if content:
        if isinstance(content, str):
            return _JSource.of(content)
        elif isinstance(content, bytes):
            # todo: can't use the no-copy path
            return _JSource.of(_JByteBuffer.wrap(content))
    elif file:
        if isinstance(file, str):
            return _JSource.of(_JFile(file))
        elif isinstance(file, Path):
            return _JSource.of(_JFile(str(file)))
    elif url:
        if isinstance(url, str):
            return _JSource.of(_JUrl(url))
    raise TypeError("todo")


def json_table(
    options: JsonValueType,
    sources: List,
    multi_value_support: bool = False,
    chunk_size: int = 2048,
    max_threads: Optional[int] = None,
) -> Table:
    builder = _JJsonTableOptions.builder()
    builder.options(json(options).j_options)
    builder.addSources(sources)
    builder.multiValueSupport(multi_value_support)
    builder.chunkSize(chunk_size)
    if max_threads:
        builder.maxThreads(max_threads)
    return Table(builder.build().execute())


def from_table(
    options: JsonValueType,
    table: Table,
    column_name: Optional[str] = None,
    chunk_size: int = 2048,
    keep_columns: Union[bool, List[str]] = False,
) -> Table:
    builder = _JTableProcessorOptions.builder()
    builder.processor(json(options).j_object)
    builder.table(table.j_table)
    if column_name:
        builder.columnName(column_name)
    builder.chunkSize(chunk_size)
    if isinstance(keep_columns, bool):
        if keep_columns:
            builder.addKeepColumns([column.name for column in table.columns])
    elif isinstance(keep_columns, List):
        builder.addKeepColumns(keep_columns)
    else:
        raise TypeError("keep_columns must be a bool or List[str]")
    return Table(builder.execute())