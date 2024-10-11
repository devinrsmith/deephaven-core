//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Empty;
import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.runner.DeephavenApiServerTestBase.TestComponent.Builder;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlClient.Savepoint;
import org.apache.arrow.flight.sql.FlightSqlClient.SubstraitPlan;
import org.apache.arrow.flight.sql.FlightSqlClient.Transaction;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedSubstraitPlanRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;
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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

// using JUnit4 so we can inherit properly from DeephavenApiServerTestBase
@RunWith(JUnit4.class)
public class FlightSqlTest extends DeephavenApiServerTestBase {

    private static final Map<String, String> DEEPHAVEN_STRING = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "java.lang.String",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> DEEPHAVEN_BYTES = Map.of(
            "deephaven:isSortable", "false",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "byte[]",
            "deephaven:componentType", "byte",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> DEEPHAVEN_INT = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "int",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> FLAT_ATTRIBUTES = Map.of(
            "deephaven:attribute_type.IsFlat", "java.lang.Boolean",
            "deephaven:attribute.IsFlat", "true");

    private static final Field CATALOG_NAME_FIELD =
            new Field("catalog_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field DB_SCHEMA_NAME =
            new Field("db_schema_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field TABLE_NAME =
            new Field("table_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field TABLE_TYPE =
            new Field("table_type", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    // private static final Field TABLE_SCHEMA =
    // new Field("table_schema", new FieldType(true, ArrowType.List.INSTANCE, null, DEEPHAVEN_BYTES),
    // List.of(Field.nullable("", MinorType.TINYINT.getType())));
    private static final Field TABLE_SCHEMA =
            new Field("table_schema", new FieldType(true, MinorType.VARBINARY.getType(), null, DEEPHAVEN_BYTES), null);

    private static final TableRef FOO_TABLE_REF = TableRef.of(null, null, "foo_table");
    public static final TableRef BAR_TABLE_REF = TableRef.of(null, null, "bar_table");

    @Module(includes = {
            TestModule.class,
            FlightSqlModule.class,
    })
    public interface MyModule {

    }

    @Singleton
    @Component(modules = MyModule.class)
    public interface MyComponent extends TestComponent {

        @Component.Builder
        interface Builder extends TestComponent.Builder {

            @BindsInstance
            Builder withServerConfig(ServerConfig serverConfig);

            @BindsInstance
            Builder withOut(@Named("out") PrintStream out);

            @BindsInstance
            Builder withErr(@Named("err") PrintStream err);

            @BindsInstance
            Builder withAuthorizationProvider(AuthorizationProvider authorizationProvider);

            MyComponent build();
        }
    }

    BufferAllocator bufferAllocator;
    ScheduledExecutorService sessionScheduler;
    FlightClient flightClient;
    FlightSqlClient flightSqlClient;

    @Override
    protected Builder testComponentBuilder() {
        return DaggerFlightSqlTest_MyComponent.builder();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        sessionScheduler = Executors.newScheduledThreadPool(2);
        bufferAllocator = new RootAllocator();
        // Note: this pattern of FlightClient owning the ManagedChannel does not mesh well with the idea that some
        // other entity may be managing the authentication lifecycle. We'd prefer to pass in the stubs or "intercepted"
        // channel directly, but that's not supported. So, we need to create the specific middleware interfaces so
        // flight can do its own shims.
        flightClient = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(bufferAllocator, channel,
                new ArrayList<>());
        // Note: this is not extensible, at least not with Auth v2 / JDBC.
        flightClient.authenticate(new ClientAuthHandler() {
            private byte[] callToken = new byte[0];

            @Override
            public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
                WrappedAuthenticationRequest request = WrappedAuthenticationRequest.newBuilder()
                        .setType("Anonymous")
                        .setPayload(ByteString.EMPTY)
                        .build();
                outgoing.send(request.toByteArray());
                callToken = incoming.next();
            }

            @Override
            public byte[] getCallToken() {
                return callToken;
            }
        });
        flightSqlClient = new FlightSqlClient(flightClient);
    }

    @Override
    public void tearDown() throws Exception {
        // this also closes flightClient
        flightSqlClient.close();
        bufferAllocator.close();
        sessionScheduler.shutdown();
        super.tearDown();
    }

    @Test
    public void listActions() {
        assertThat(flightClient.listActions())
                .usingElementComparator(Comparator.comparing(ActionType::getType))
                .containsExactlyInAnyOrder(
                        FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
                        FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
    }

    @Test
    public void getCatalogs() throws Exception {
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME_FIELD);
        {
            final SchemaResult schemaResult = flightSqlClient.getCatalogsSchema();
            assertThat(schemaResult.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getCatalogs();
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                consume(stream, 0, 0);
            }
        }
        unpackable(CommandGetCatalogs.getDescriptor(), CommandGetCatalogs.class);
    }

    @Test
    public void getSchemas() throws Exception {
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME);
        {
            final SchemaResult schemasSchema = flightSqlClient.getSchemasSchema();
            assertThat(schemasSchema.getSchema()).isEqualTo(expectedSchema);
        }
        for (final FlightInfo info : new FlightInfo[] {
                flightSqlClient.getSchemas(null, null),
                flightSqlClient.getSchemas("DoesNotExist", null)}) {
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                consume(stream, 0, 0);
            }
        }
        expectException(() -> flightSqlClient.getSchemas(null, "filter_pattern"), FlightStatusCode.INVALID_ARGUMENT,
                "FlightSQL arrow.flight.protocol.sql.CommandGetDbSchemas.db_schema_filter_pattern not supported at this time");
        unpackable(CommandGetDbSchemas.getDescriptor(), CommandGetDbSchemas.class);
    }

    @Test
    public void getTables() throws Exception {
        setFooTable();
        setBarTable();
        for (final boolean includeSchema : new boolean[] {false, true}) {
            final Schema expectedSchema = includeSchema
                    ? flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA)
                    : flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE);
            {
                final SchemaResult schema = flightSqlClient.getTablesSchema(includeSchema);
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            // Any of these queries will fetch everything from query scope
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables(null, null, null, null, includeSchema),
                    flightSqlClient.getTables("", null, null, null, includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("TABLE"), includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("IRRELEVANT_TYPE", "TABLE"), includeSchema),
                    flightSqlClient.getTables("", null, null, List.of("TABLE"), includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                    consume(stream, 1, 2);
                }
            }
            // Any of these queries will fetch an empty table
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables("DoesNotExistCatalog", null, null, null, includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("IRRELEVANT_TYPE"), includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                    consume(stream, 0, 0);
                }
            }
            // We do not implement filtering right now
            expectException(() -> flightSqlClient.getTables(null, "filter_pattern", null, null, includeSchema),
                    FlightStatusCode.INVALID_ARGUMENT,
                    "FlightSQL arrow.flight.protocol.sql.CommandGetTables.db_schema_filter_pattern not supported at this time");
            expectException(() -> flightSqlClient.getTables(null, null, "filter_pattern", null, includeSchema),
                    FlightStatusCode.INVALID_ARGUMENT,
                    "FlightSQL arrow.flight.protocol.sql.CommandGetTables.table_name_filter_pattern not supported at this time");
        }
        unpackable(CommandGetTables.getDescriptor(), CommandGetTables.class);
    }

    @Test
    public void getTableTypes() throws Exception {
        final Schema expectedSchema = flatTableSchema(TABLE_TYPE);
        {
            final SchemaResult schema = flightSqlClient.getTableTypesSchema();
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getTableTypes();
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                consume(stream, 1, 1);
            }
        }
        unpackable(CommandGetTableTypes.getDescriptor(), CommandGetTableTypes.class);
    }

    @Test
    public void select1() throws Exception {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        {
            final SchemaResult schema = flightSqlClient.getExecuteSchema("SELECT 1 as Foo");
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.execute("SELECT 1 as Foo");
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                consume(stream, 1, 1);
            }
        }
        unpackable(CommandStatementQuery.getDescriptor(), CommandStatementQuery.class);
    }

    @Test
    public void select1Prepared() throws Exception {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final PreparedStatement preparedStatement = flightSqlClient.prepare("SELECT 1 as Foo")) {
            {
                final SchemaResult schema = preparedStatement.fetchSchema();
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo info = preparedStatement.execute();
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                    consume(stream, 1, 1);
                }
            }
            unpackable(CommandPreparedStatementQuery.getDescriptor(), CommandPreparedStatementQuery.class);
        }
    }

    @Test
    public void selectStarFromQueryScopeTable() throws Exception {
        setFooTable();
        {
            final Schema expectedSchema = flatTableSchema(
                    new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null));
            {
                final SchemaResult schema = flightSqlClient.getExecuteSchema("SELECT * FROM foo_table");
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo info = flightSqlClient.execute("SELECT * FROM foo_table");
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                    consume(stream, 1, 3);
                }
            }
            unpackable(CommandStatementQuery.getDescriptor(), CommandStatementQuery.class);
        }
    }

    @Test
    public void selectStarPreparedFromQueryScopeTable() throws Exception {
        setFooTable();
        {
            final Schema expectedSchema = flatTableSchema(
                    new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null));
            try (final PreparedStatement prepared = flightSqlClient.prepare("SELECT * FROM foo_table")) {
                {
                    final SchemaResult schema = prepared.fetchSchema();
                    assertThat(schema.getSchema()).isEqualTo(expectedSchema);
                }
                {
                    final FlightInfo info = prepared.execute();
                    assertThat(info.getSchema()).isEqualTo(expectedSchema);
                    try (final FlightStream stream = flightSqlClient.getStream(ticket(info))) {
                        consume(stream, 1, 3);
                    }
                }
                unpackable(CommandPreparedStatementQuery.getDescriptor(), CommandPreparedStatementQuery.class);
            }
            unpackable(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT, ActionCreatePreparedStatementRequest.class);
            unpackable(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT, ActionClosePreparedStatementRequest.class);
        }
    }

    @Test
    public void executeSubstrait() {
        getSchemaUnimplemented(() -> flightSqlClient.getExecuteSubstraitSchema(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.executeSubstrait(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        unpackable(CommandStatementSubstraitPlan.getDescriptor(), CommandStatementSubstraitPlan.class);
    }

    @Test
    public void executeSubstraitUpdate() {
        // Note: this is the same descriptor as the executeSubstrait
        getSchemaUnimplemented(() -> flightSqlClient.getExecuteSubstraitSchema(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        expectUnpublishable(() -> flightSqlClient.executeSubstraitUpdate(fakePlan()));
        unpackable(CommandStatementSubstraitPlan.getDescriptor(), CommandStatementSubstraitPlan.class);
    }

    @Test
    public void insert1() {
        expectUnpublishable(() -> flightSqlClient.executeUpdate("INSERT INTO fake(name) VALUES('Smith')"));
        unpackable(CommandStatementUpdate.getDescriptor(), CommandStatementUpdate.class);
    }

    @Ignore("need to fix server, should error out before")
    @Test
    public void insert1Prepared() {
        try (final PreparedStatement prepared = flightSqlClient.prepare("INSERT INTO fake(name) VALUES('Smith')")) {
            // final SchemaResult schema = prepared.fetchSchema();
            // // TODO: note the lack of a useful error from perspective of client.
            // // INVALID_ARGUMENT: Export in state DEPENDENCY_FAILED
            // //
            // // final SessionState.ExportObject<Flight.FlightInfo> export =
            // // ticketRouter.flightInfoFor(session, request, "request");
            // //
            // // if (session != null) {
            // // session.nonExport()
            // // .queryPerformanceRecorder(queryPerformanceRecorder)
            // // .require(export)
            // // .onError(responseObserver)
            // // .submit(() -> {
            // // responseObserver.onNext(export.get());
            // // responseObserver.onCompleted();
            // // });
            // // return;
            // // }
            //
            // unpackable(CommandPreparedStatementUpdate.getDescriptor(), CommandPreparedStatementUpdate.class);
        }
    }

    @Test
    public void getSqlInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getSqlInfoSchema(), CommandGetSqlInfo.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getSqlInfo(), CommandGetSqlInfo.getDescriptor());
        unpackable(CommandGetSqlInfo.getDescriptor(), CommandGetSqlInfo.class);
    }

    @Test
    public void getXdbcTypeInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getXdbcTypeInfoSchema(), CommandGetXdbcTypeInfo.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getXdbcTypeInfo(), CommandGetXdbcTypeInfo.getDescriptor());
        unpackable(CommandGetXdbcTypeInfo.getDescriptor(), CommandGetXdbcTypeInfo.class);
    }

    @Test
    public void getCrossReference() {
        setFooTable();
        setBarTable();
        getSchemaUnimplemented(() -> flightSqlClient.getCrossReferenceSchema(),
                CommandGetCrossReference.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getCrossReference(FOO_TABLE_REF, BAR_TABLE_REF),
                CommandGetCrossReference.getDescriptor());
        unpackable(CommandGetCrossReference.getDescriptor(), CommandGetCrossReference.class);
    }

    @Test
    public void getPrimaryKeys() {
        setFooTable();
        getSchemaUnimplemented(() -> flightSqlClient.getPrimaryKeysSchema(), CommandGetPrimaryKeys.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getPrimaryKeys(FOO_TABLE_REF),
                CommandGetPrimaryKeys.getDescriptor());
        unpackable(CommandGetPrimaryKeys.getDescriptor(), CommandGetPrimaryKeys.class);
    }

    @Test
    public void getExportedKeys() {
        setFooTable();
        getSchemaUnimplemented(() -> flightSqlClient.getExportedKeysSchema(), CommandGetExportedKeys.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getExportedKeys(FOO_TABLE_REF),
                CommandGetExportedKeys.getDescriptor());
        unpackable(CommandGetExportedKeys.getDescriptor(), CommandGetExportedKeys.class);
    }

    @Test
    public void getImportedKeys() {
        setFooTable();
        getSchemaUnimplemented(() -> flightSqlClient.getImportedKeysSchema(), CommandGetImportedKeys.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getImportedKeys(FOO_TABLE_REF),
                CommandGetImportedKeys.getDescriptor());
        unpackable(CommandGetImportedKeys.getDescriptor(), CommandGetImportedKeys.class);
    }

    @Test
    public void commandStatementIngest() {
        // This is a real newer FlightSQL command.
        // Once we upgrade to newer FlightSQL, we can change this to Unimplemented and use the proper APIs.
        final String typeUrl = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementIngest";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        getSchemaUnknown(() -> flightClient.getSchema(descriptor), typeUrl);
        commandUnknown(() -> flightClient.getInfo(descriptor), typeUrl);
    }

    @Test
    public void unknownCommandLooksLikeFlightSql() {
        final String typeUrl = "type.googleapis.com/arrow.flight.protocol.sql.CommandLooksRealButDoesNotExist";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        getSchemaUnknown(() -> flightClient.getSchema(descriptor), typeUrl);
        commandUnknown(() -> flightClient.getInfo(descriptor), typeUrl);
    }

    @Test
    public void unknownCommand() {
        // Note: this should likely be tested in the context of Flight, not FlightSQL
        final String typeUrl = "type.googleapis.com/com.example.SomeRandomCommand";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        expectException(() -> flightClient.getSchema(descriptor), FlightStatusCode.INVALID_ARGUMENT,
                "no resolver for command");
        expectException(() -> flightClient.getInfo(descriptor), FlightStatusCode.INVALID_ARGUMENT,
                "no resolver for command");
    }

    @Test
    public void prepareSubstrait() {
        actionUnimplemented(() -> flightSqlClient.prepare(fakePlan()),
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN);
        unpackable(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN,
                ActionCreatePreparedSubstraitPlanRequest.class);
    }

    @Test
    public void beginTransaction() {
        actionUnimplemented(() -> flightSqlClient.beginTransaction(), FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION, ActionBeginTransactionRequest.class);
    }

    @Test
    public void commit() {
        actionUnimplemented(() -> flightSqlClient.commit(fakeTxn()), FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION, ActionEndTransactionRequest.class);
    }

    @Test
    public void rollbackTxn() {
        actionUnimplemented(() -> flightSqlClient.rollback(fakeTxn()), FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION, ActionEndTransactionRequest.class);
    }

    @Test
    public void beginSavepoint() {
        actionUnimplemented(() -> flightSqlClient.beginSavepoint(fakeTxn(), "fakeName"),
                FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT, ActionBeginSavepointRequest.class);
    }

    @Test
    public void release() {
        actionUnimplemented(() -> flightSqlClient.release(fakeSavepoint()), FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT, ActionEndSavepointRequest.class);
    }

    @Test
    public void rollbackSavepoint() {
        actionUnimplemented(() -> flightSqlClient.rollback(fakeSavepoint()), FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT, ActionEndSavepointRequest.class);
    }

    @Test
    public void cancelQuery() {
        final FlightInfo info = flightSqlClient.execute("SELECT 1");
        actionUnimplemented(() -> flightSqlClient.cancelQuery(info), FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY);
        unpackable(FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY, ActionCancelQueryRequest.class);
    }

    @Test
    public void cancelFlightInfo() {
        // Note: this should likely be tested in the context of Flight, not FlightSQL
        final FlightInfo info = flightSqlClient.execute("SELECT 1");
        actionNoResolver(() -> flightClient.cancelFlightInfo(new CancelFlightInfoRequest(info)),
                FlightConstants.CANCEL_FLIGHT_INFO.getType());
    }

    @Test
    public void unknownAction() {
        // Note: this should likely be tested in the context of Flight, not FlightSQL
        final String type = "SomeFakeAction";
        final Action action = new Action(type, new byte[0]);
        actionNoResolver(() -> doAction(action), type);
    }

    private Result doAction(Action action) {
        final Iterator<Result> it = flightClient.doAction(action);
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final Result result = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return result;
    }

    private static FlightDescriptor unpackableCommand(Descriptor descriptor) {
        return unpackableCommand("type.googleapis.com/" + descriptor.getFullName());
    }

    private static FlightDescriptor unpackableCommand(String typeUrl) {
        return FlightDescriptor.command(
                Any.newBuilder().setTypeUrl(typeUrl).setValue(ByteString.copyFrom(new byte[1])).build().toByteArray());
    }

    private void getSchemaUnimplemented(Runnable r, Descriptor command) {
        // right now our server impl routes all getSchema through their respective commands
        commandUnimplemented(r, command);
    }

    private void commandUnimplemented(Runnable r, Descriptor command) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("FlightSQL command '%s' is unimplemented", command.getFullName()));
    }

    private void getSchemaUnknown(Runnable r, String command) {
        // right now our server impl routes all getSchema through their respective commands
        commandUnknown(r, command);
    }

    private void commandUnknown(Runnable r, String command) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED, String.format("FlightSQL command '%s' is unknown", command));
    }

    private void unpackable(Descriptor descriptor, Class<?> clazz) {
        final FlightDescriptor flightDescriptor = unpackableCommand(descriptor);
        getSchemaUnpackable(() -> flightClient.getSchema(flightDescriptor), clazz);
        commandUnpackable(() -> flightClient.getInfo(flightDescriptor), clazz);
    }

    private void unpackable(ActionType type, Class<?> actionProto) {
        {
            final Action action = new Action(type.getType(), Any.getDefaultInstance().toByteArray());
            expectUnpackable(() -> doAction(action), actionProto);
        }
        {
            final Action action = new Action(type.getType(), new byte[] {-1});
            expectException(() -> doAction(action), FlightStatusCode.INVALID_ARGUMENT,
                    "Received invalid message from remote");
        }
    }

    private void getSchemaUnpackable(Runnable r, Class<?> clazz) {
        commandUnpackable(r, clazz);
    }

    private void commandUnpackable(Runnable r, Class<?> clazz) {
        expectUnpackable(r, clazz);
    }

    private void expectUnpackable(Runnable r, Class<?> clazz) {
        expectException(r, FlightStatusCode.INVALID_ARGUMENT,
                String.format("Provided message cannot be unpacked as %s", clazz.getName()));
    }

    private void expectUnpublishable(Runnable r) {
        expectException(r, FlightStatusCode.INVALID_ARGUMENT, "FlightSQL descriptors cannot be published to");
    }

    private void actionUnimplemented(Runnable r, ActionType actionType) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("FlightSQL Action type '%s' is unimplemented", actionType.getType()));
    }

    private void actionNoResolver(Runnable r, String actionType) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("No action resolver found for action type '%s'", actionType));
    }

    private void expectException(Runnable r, FlightStatusCode code, String messagePart) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            assertThat(e.status().code()).isEqualTo(code);
            assertThat(e).hasMessageContaining(messagePart);
        }
    }

    private static Ticket ticket(FlightInfo info) {
        assertThat(info.getEndpoints()).hasSize(1);
        return info.getEndpoints().get(0).getTicket();
    }

    private static Schema flatTableSchema(Field... fields) {
        return new Schema(List.of(fields), FLAT_ATTRIBUTES);
    }

    private static void setFooTable() {
        setSimpleTable("foo_table", "Foo");
    }

    private static void setBarTable() {
        setSimpleTable("bar_table", "Bar");
    }

    private static void setSimpleTable(String tableName, String columnName) {
        final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(columnName));
        final Table table = TableTools.newTable(td, TableTools.intCol(columnName, 1, 2, 3));
        ExecutionContext.getContext().getQueryScope().putParam(tableName, table);
    }

    private static void consume(FlightStream stream, int expectedFlightCount, int expectedNumRows) {
        int numRows = 0;
        int flightCount = 0;
        while (stream.next()) {
            ++flightCount;
            numRows += stream.getRoot().getRowCount();
        }
        assertThat(flightCount).isEqualTo(expectedFlightCount);
        assertThat(numRows).isEqualTo(expectedNumRows);
    }

    private static SubstraitPlan fakePlan() {
        return new SubstraitPlan("fake".getBytes(StandardCharsets.UTF_8), "1");
    }

    private static Transaction fakeTxn() {
        return new Transaction("fake".getBytes(StandardCharsets.UTF_8));
    }

    private static Savepoint fakeSavepoint() {
        return new Savepoint("fake".getBytes(StandardCharsets.UTF_8));
    }
}