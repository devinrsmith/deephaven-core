package io.deephaven.engine.sql;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutput.ObjFormatter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.util.AbstractScriptSession.ScriptSessionQueryScope;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Graphviz;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableHeader.Builder;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.sql.Scope;
import io.deephaven.sql.ScopeStaticImpl;
import io.deephaven.sql.SqlAdapter;
import io.deephaven.sql.TableInformation;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.ScriptApi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Experimental SQL execution. Subject to change.
 */
public final class Sql {
    private static final Logger log = LoggerFactory.getLogger(Sql.class);

    @ScriptApi
    public static Table executeSql(String sql) {
        return executeSql(sql, scriptSessionScope());
    }

    public static Table executeSql(String sql, Map<String, Table> scope) {
        final Map<TicketTable, Table> mapOut = new HashMap<>(scope.size());
        final TableSpec tableSpec = SqlAdapter.parseSql(sql, scope(scope, mapOut));
        log.debug().append("Executing. Graphviz representation:").nl().append(ToGraphvizDot.INSTANCE, tableSpec).endl();
        return tableSpec.logic().create(new TableCreatorTicketInterceptor(TableCreatorImpl.INSTANCE, mapOut));
    }

    private static Scope scope(Map<String, Table> scope, Map<TicketTable, Table> out) {
        final ScopeStaticImpl.Builder builder = ScopeStaticImpl.builder();
        for (Entry<String, Table> e : scope.entrySet()) {
            final String tableName = e.getKey();
            final Table table = e.getValue();
            // The TicketTable can technically be anything unique (incrementing number, random, ...), but for
            // visualization purposes it makes sense to use the (already unique) table name.
            final TicketTable spec = TicketTable.of("sqlref/" + tableName);
            final List<String> qualifiedName = List.of(tableName);
            final TableHeader header = adapt(table.getDefinition());
            builder.addTables(TableInformation.of(qualifiedName, header, spec));
            out.put(spec, table);
        }
        return builder.build();
    }

    private static Map<String, Table> scriptSessionScope() {
        final Map<String, Table> scope = new HashMap<>();
        // getVariables() is inefficient
        // See SQLTODO(catalog-reader-implementation)
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
