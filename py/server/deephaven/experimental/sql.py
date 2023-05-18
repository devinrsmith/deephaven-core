#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import jpy

from deephaven import DHError
from deephaven.table import Table


_JSql = jpy.get_type("io.deephaven.engine.sql.Sql")


def execute_sql(sql: str) -> Table:
    try:
        j_table = _JSql.executeSql(sql)
        return Table(j_table=j_table)
    except Exception as e:
        raise DHError(e, "failed to execute SQL.") from e
