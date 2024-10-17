//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetCatalogsImpl;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetDbSchemasImpl;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetTableTypesImpl;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetTablesImpl;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlightSqlTicketResolverTest {
    @Test
    public void actionTypes() {
        checkActionType(FlightSqlResolver.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        checkActionType(FlightSqlResolver.CLOSE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
        checkActionType(FlightSqlResolver.BEGIN_SAVEPOINT_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT);
        checkActionType(FlightSqlResolver.END_SAVEPOINT_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        checkActionType(FlightSqlResolver.BEGIN_TRANSACTION_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION);
        checkActionType(FlightSqlResolver.END_TRANSACTION_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        checkActionType(FlightSqlResolver.CANCEL_QUERY_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY);
        checkActionType(FlightSqlResolver.CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN);
    }

    @Test
    public void commandTypeUrls() {
        checkPackedType(FlightSqlResolver.COMMAND_STATEMENT_QUERY_TYPE_URL,
                CommandStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_STATEMENT_UPDATE_TYPE_URL,
                CommandStatementUpdate.getDefaultInstance());
        // Need to update to newer FlightSql version for this
        // checkPackedType(FlightSqlTicketResolver.COMMAND_STATEMENT_INGEST_TYPE_URL,
        // CommandStatementIngest.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL,
                CommandStatementSubstraitPlan.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL,
                CommandPreparedStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL,
                CommandPreparedStatementUpdate.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_TABLE_TYPES_TYPE_URL,
                CommandGetTableTypes.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_CATALOGS_TYPE_URL,
                CommandGetCatalogs.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_DB_SCHEMAS_TYPE_URL,
                CommandGetDbSchemas.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_TABLES_TYPE_URL,
                CommandGetTables.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_SQL_INFO_TYPE_URL,
                CommandGetSqlInfo.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_CROSS_REFERENCE_TYPE_URL,
                CommandGetCrossReference.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_EXPORTED_KEYS_TYPE_URL,
                CommandGetExportedKeys.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_IMPORTED_KEYS_TYPE_URL,
                CommandGetImportedKeys.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_PRIMARY_KEYS_TYPE_URL,
                CommandGetPrimaryKeys.getDefaultInstance());
        checkPackedType(FlightSqlResolver.COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL,
                CommandGetXdbcTypeInfo.getDefaultInstance());
        checkPackedType(FlightSqlResolver.TICKET_STATEMENT_QUERY_TYPE_URL,
                TicketStatementQuery.getDefaultInstance());
    }

    @Test
    void definitions() {
        checkDefinition(CommandGetTableTypesImpl.DEFINITION, Schemas.GET_TABLE_TYPES_SCHEMA);
        checkDefinition(CommandGetCatalogsImpl.DEFINITION, Schemas.GET_CATALOGS_SCHEMA);
        checkDefinition(CommandGetDbSchemasImpl.DEFINITION, Schemas.GET_SCHEMAS_SCHEMA);
        // TODO: we can't use the straight schema b/c it's BINARY not byte[], and we don't know how to natively map
        // checkDefinition(CommandGetTablesImpl.DEFINITION, Schemas.GET_TABLES_SCHEMA);
        checkDefinition(CommandGetTablesImpl.DEFINITION_NO_SCHEMA, Schemas.GET_TABLES_SCHEMA_NO_SCHEMA);

    }

    private static void checkActionType(String actionType, ActionType expected) {
        assertThat(actionType).isEqualTo(expected.getType());
    }

    private static void checkPackedType(String typeUrl, Message expected) {
        assertThat(typeUrl).isEqualTo(Any.pack(expected).getTypeUrl());
    }

    private static void checkDefinition(TableDefinition definition, Schema expected) {
        assertThat(definition).isEqualTo(BarrageUtil.convertArrowSchema(expected).tableDef);
    }
}
