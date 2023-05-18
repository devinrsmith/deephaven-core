package io.deephaven.engine.sql;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutput.ObjFormatter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.AbstractScriptSession.ScriptSessionQueryScope;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Graphviz;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableHeader.Builder;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.sql.SqlAdapter;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.ScriptApi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Experimental SQL execution.
 */
public final class Sql {
    private static final Logger log = LoggerFactory.getLogger(Sql.class);

    @ScriptApi
    public static Table executeSql(String sql) {
        return executeSql(sql, scope());
    }

    public static Table executeSql(String sql, Map<String, Table> scope) {
        return execute(parseSql(sql, scope), scope);
    }

    public static TableSpec parseSql(String sql, Map<String, Table> scope) {
        final Map<List<String>, TableHeader> headers = new HashMap<>();
        for (Entry<String, Table> e : scope.entrySet()) {
            // todo: qualify different scopes?
            headers.put(List.of(e.getKey()), adapt(e.getValue().getDefinition()));
        }
        return SqlAdapter.parseSql(sql, headers);
    }

    public static Table execute(TableSpec tableSpec, Map<String, Table> scope) {
        log.debug().append("Executing ").append(ToGraphvizDot.INSTANCE, tableSpec).endl();
        return tableSpec.logic().create(new SqlScanSupport(scope));
    }

    private static Map<String, Table> scope() {
        final Map<String, Table> scope = new HashMap<>();
        // getVariables() is inefficient
        // Also, see note in io.deephaven.sql.SqlAdapter
        for (Entry<String, Object> e : currentScriptSession().getVariables().entrySet()) {
            if (e.getValue() instanceof Table) {
                scope.put(e.getKey(), (Table) e.getValue());
            }
        }
        return scope;
    }

    private static ScriptSession currentScriptSession() {
        return ((ScriptSessionQueryScope) ExecutionContext.getContext().getQueryScope()).scriptSession();
    }

    private static TableHeader adapt(TableDefinition tableDef) {
        final Builder builder = TableHeader.builder();
        for (ColumnDefinition<?> cd : tableDef.getColumns()) {
            builder.addHeaders(adapt(cd));
        }
        return builder.build();
    }

    private static ColumnHeader<?> adapt(ColumnDefinition<?> columnDef) {
        if (columnDef.getComponentType() == null) {
            if (DateTime.class.equals(columnDef.getDataType())) {
                return ColumnHeader.ofInstant(columnDef.getName());
            }
            return ColumnHeader.of(columnDef.getName(), columnDef.getDataType());
        }
        throw new UnsupportedOperationException("todo, adapt array / vector types");
    }

    private enum ToGraphvizDot implements ObjFormatter<TableSpec> {
        INSTANCE;

        @Override
        public void format(LogOutput logOutput, TableSpec tableSpec) {
            logOutput.append(Graphviz.toDot(tableSpec));
        }
    }
}
