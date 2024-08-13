#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

""" This module implements the Column class and functions that work with Columns. """

from enum import Enum
from functools import cached_property
from typing import Sequence, Any

import jpy

import deephaven.dtypes as dtypes
from deephaven import DHError
from deephaven.dtypes import DType, _instant_array, from_jtype
from deephaven._wrapper import JObjectWrapper

_JColumnHeader = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
_JColumn = jpy.get_type("io.deephaven.qst.column.Column")
_JColumnDefinition = jpy.get_type("io.deephaven.engine.table.ColumnDefinition")
_JColumnDefinitionType = jpy.get_type("io.deephaven.engine.table.ColumnDefinition$ColumnType")
_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")


class ColumnType(Enum):
    NORMAL = _JColumnDefinitionType.Normal
    """ A regular column. """
    PARTITIONING = _JColumnDefinitionType.Partitioning
    """ A partitioning column. """

    def __repr__(self):
        return self.name


class ColumnDefinition(JObjectWrapper):
    """A Deephaven column definition."""

    j_object_type = _JColumnDefinition

    @staticmethod
    def of(
            name: str,
            data_type: DType,
            component_type: DType = None,
            column_type: ColumnType = ColumnType.NORMAL,
    ) -> 'ColumnDefinition':
        return ColumnDefinition(
            _JColumnDefinition.fromGenericType(
                name,
                data_type.j_type.jclass
                if hasattr(data_type.j_type, "jclass")
                else data_type.qst_type.clazz(),
                component_type.qst_type.clazz() if component_type else None,
                column_type.value,
            )
        )

    def __init__(self, j_column_definition: jpy.JType):
        self.j_column_definition = j_column_definition

    @property
    def j_object(self) -> jpy.JType:
        return self.j_column_definition

    @cached_property
    def name(self) -> str:
        return self.j_column_definition.getName()

    @cached_property
    def data_type(self) -> DType:
        return from_jtype(self.j_column_definition.getDataType())

    @cached_property
    def component_type(self) -> DType:
        return from_jtype(self.j_column_definition.getComponentType())

    @cached_property
    def column_type(self) -> ColumnType:
        return ColumnType(self.j_column_definition.getColumnType())


class Column(ColumnDefinition):
    """A Column object represents a column definition in a Deephaven Table."""

    def __init__(
            self,
            name: str,
            data_type: DType,
            component_type: DType = None,
            column_type: ColumnType = ColumnType.NORMAL,
    ):
        super().__init__(
            ColumnDefinition.of(
                name, data_type, component_type, column_type
            ).j_column_definition
        )


class InputColumn:
    """An InputColumn represents a user defined column with some input data."""

    def __init__(
            self,
            name: str = None,
            data_type: DType = None,
            component_type: DType = None,
            column_type: ColumnType = ColumnType.NORMAL,
            input_data: Any = None,
    ):
        try:
            self.column_definition = ColumnDefinition.of(
                name, data_type, component_type, column_type
            )
            self.j_column = self._to_j_column(input_data)
        except Exception as e:
            raise DHError(e, f"failed to create an InputColumn ({self.name}).") from e

    @property
    def name(self):
        return self.column_definition.name

    @property
    def data_type(self) -> DType:
        return self.column_definition.data_type

    @property
    def component_type(self) -> DType:
        return self.column_definition.component_type

    @property
    def column_type(self) -> ColumnType:
        return self.column_definition.column_type

    def _to_j_column(self, input_data: Any = None) -> jpy.JType:
        if input_data is None:
            return _JColumn.empty(_JColumnHeader.of(self.name, self.data_type.qst_type))
        if self.data_type.is_primitive:
            return _JColumn.ofUnsafe(
                self.name,
                dtypes.array(
                    self.data_type,
                    input_data,
                    remap=dtypes.null_remap(self.data_type),
                ),
            )
        return _JColumn.of(
            _JColumnHeader.of(self.name, self.data_type.qst_type),
            dtypes.array(self.data_type, input_data),
        )


def bool_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Boolean data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.bool_, input_data=data)


def byte_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive byte data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.byte, input_data=data)


def char_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive char data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.char, input_data=data)


def short_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive short data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.short, input_data=data)


def int_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive int data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.int32, input_data=data)


def long_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive long data.

    Args:
        name (str): the column name
        data (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.long, input_data=data)


def float_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive float data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.float32, input_data=data)


def double_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive double data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.double, input_data=data)


def string_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing string data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.string, input_data=data)


def datetime_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Deephaven Datetime instances.

    Args:
        name (str): the column name
        data (Any): a sequence of Datetime instances or values that can be converted to Datetime instances
            (e.g. Instant, int nanoseconds since the Epoch, str, datetime.datetime, numpy.datetime64, pandas.Timestamp).

    Returns:
        a new input column
    """
    data = _instant_array(data)
    return InputColumn(name=name, data_type=dtypes.Instant, input_data=data)


def pyobj_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column containing complex, non-primitive-like Python objects.

    Args:
        name (str): the column name
        data (Any): a sequence of Python objects

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.PyObject, input_data=data)


def jobj_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Java objects.

    Args:
        name (str): the column name
        data (Any): a sequence of Java objects

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.JObject, input_data=data)
