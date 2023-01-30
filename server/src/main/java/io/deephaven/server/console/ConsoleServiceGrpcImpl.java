/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.console;

import com.google.rpc.Code;
import io.deephaven.base.RingBuffer;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.engine.table.impl.util.RuntimeMemory.Sample;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.util.DelegatingScriptSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.integrations.python.PythonDeephavenSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.io.logger.LogBufferRecordListener;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.server.console.completer.JavaAutoCompleteObserver;
import io.deephaven.server.console.completer.PythonAutoCompleteObserver;
import io.deephaven.server.session.SessionCloseableObserver;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportBuilder;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.util.Scheduler;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jpy.PyObject;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyOnNext;

@Singleton
public class ConsoleServiceGrpcImpl extends ConsoleServiceGrpc.ConsoleServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcImpl.class);

    public static final boolean REMOTE_CONSOLE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.disable", false);

    public static final boolean AUTOCOMPLETE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.autocomplete.disable", false);

    public static final boolean QUIET_AUTOCOMPLETE_ERRORS =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.autocomplete.quiet", true);

    public static final long SUBSCRIBE_TO_LOGS_SEND_MILLIS =
            Configuration.getInstance().getLongWithDefault("deephaven.console.subscribeToLogs.send_millis", 100);

    public static final int SUBSCRIBE_TO_LOGS_BUFFER_SIZE =
            Configuration.getInstance().getIntegerWithDefault("deephaven.console.subscribeToLogs.buffer_size", 32768);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final Provider<ScriptSession> scriptSessionProvider;
    private final Scheduler scheduler;
    private final LogBuffer logBuffer;

    @Inject
    public ConsoleServiceGrpcImpl(final TicketRouter ticketRouter,
            final SessionService sessionService,
            final Provider<ScriptSession> scriptSessionProvider,
            final Scheduler scheduler,
            final LogBuffer logBuffer) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.scriptSessionProvider = scriptSessionProvider;
        this.scheduler = Objects.requireNonNull(scheduler);
        this.logBuffer = Objects.requireNonNull(logBuffer);
    }

    @Override
    public void getConsoleTypes(final GetConsoleTypesRequest request,
            final StreamObserver<GetConsoleTypesResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            // TODO (deephaven-core#3147): for legacy reasons, this method is used prior to authentication
            if (!REMOTE_CONSOLE_DISABLED) {
                responseObserver.onNext(GetConsoleTypesResponse.newBuilder()
                        .addConsoleTypes(scriptSessionProvider.get().scriptType().toLowerCase())
                        .build());
            } else {
                responseObserver.onNext(GetConsoleTypesResponse.getDefaultInstance());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void startConsole(StartConsoleRequest request, StreamObserver<StartConsoleResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            SessionState session = sessionService.getCurrentSession();
            if (REMOTE_CONSOLE_DISABLED) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Remote console disabled"));
                return;
            }

            // TODO (#702): initially global session will be null; set it here if applicable

            final String sessionType = request.getSessionType();
            if (!scriptSessionProvider.get().scriptType().equalsIgnoreCase(sessionType)) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "session type '" + sessionType + "' is not supported");
            }

            session.newExport(request.getResultId(), "resultId")
                    .onError(responseObserver)
                    .submit(() -> {
                        final ScriptSession scriptSession = new DelegatingScriptSession(scriptSessionProvider.get());

                        safelyComplete(responseObserver, StartConsoleResponse.newBuilder()
                                .setResultId(request.getResultId())
                                .build());

                        return scriptSession;
                    });
        });
    }

    @Override
    public void subscribeToLogs(LogSubscriptionRequest request, StreamObserver<LogSubscriptionData> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (REMOTE_CONSOLE_DISABLED) {
                GrpcUtil.safelyError(responseObserver, Code.FAILED_PRECONDITION, "Remote console disabled");
                return;
            }
            final LogsClient client = new LogsClient(request, responseObserver);
            client.start();
        });
    }

    @Override
    public void executeCommand(ExecuteCommandRequest request, StreamObserver<ExecuteCommandResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final Ticket consoleId = request.getConsoleId();
            if (consoleId.getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No consoleId supplied");
            }

            SessionState.ExportObject<ScriptSession> exportedConsole =
                    ticketRouter.resolve(session, consoleId, "consoleId");
            session.nonExport()
                    .requiresSerialQueue()
                    .require(exportedConsole)
                    .onError(responseObserver)
                    .submit(() -> {
                        ScriptSession scriptSession = exportedConsole.get();
                        ScriptSession.Changes changes = scriptSession.evaluateScript(request.getCode());
                        ExecuteCommandResponse.Builder diff = ExecuteCommandResponse.newBuilder();
                        FieldsChangeUpdate.Builder fieldChanges = FieldsChangeUpdate.newBuilder();
                        changes.created.entrySet()
                                .forEach(entry -> fieldChanges.addCreated(makeVariableDefinition(entry)));
                        changes.updated.entrySet()
                                .forEach(entry -> fieldChanges.addUpdated(makeVariableDefinition(entry)));
                        changes.removed.entrySet()
                                .forEach(entry -> fieldChanges.addRemoved(makeVariableDefinition(entry)));
                        responseObserver.onNext(diff.setChanges(fieldChanges).build());
                        responseObserver.onCompleted();
                    });
        });
    }

    @Override
    public void getHeapInfo(GetHeapInfoRequest request, StreamObserver<GetHeapInfoResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
            final Sample sample = new Sample();
            runtimeMemory.read(sample);
            final GetHeapInfoResponse infoResponse = GetHeapInfoResponse.newBuilder()
                    .setTotalMemory(sample.totalMemory)
                    .setFreeMemory(sample.freeMemory)
                    .setMaxMemory(runtimeMemory.maxMemory())
                    .build();
            responseObserver.onNext(infoResponse);
            responseObserver.onCompleted();
        });
    }

    private static FieldInfo makeVariableDefinition(Map.Entry<String, String> entry) {
        return makeVariableDefinition(entry.getKey(), entry.getValue());
    }

    private static FieldInfo makeVariableDefinition(String title, String type) {
        final TypedTicket id = TypedTicket.newBuilder()
                .setType(type)
                .setTicket(ScopeTicketResolver.ticketForName(title))
                .build();
        return FieldInfo.newBuilder()
                .setApplicationId("scope")
                .setFieldName(title)
                .setFieldDescription("query scope variable")
                .setTypedTicket(id)
                .build();
    }

    @Override
    public void cancelCommand(CancelCommandRequest request, StreamObserver<CancelCommandResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            // TODO (#53): consider task cancellation
            super.cancelCommand(request, responseObserver);
        });
    }

    @Override
    public void bindTableToVariable(BindTableToVariableRequest request,
            StreamObserver<BindTableToVariableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            Ticket tableId = request.getTableId();
            if (tableId.getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No source tableId supplied");
            }
            final SessionState.ExportObject<Table> exportedTable = ticketRouter.resolve(session, tableId, "tableId");
            final SessionState.ExportObject<ScriptSession> exportedConsole;

            ExportBuilder<?> exportBuilder = session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver);

            if (request.hasConsoleId()) {
                exportedConsole = ticketRouter.resolve(session, request.getConsoleId(), "consoleId");
                exportBuilder.require(exportedTable, exportedConsole);
            } else {
                exportedConsole = null;
                exportBuilder.require(exportedTable);
            }

            exportBuilder.submit(() -> {
                ScriptSession scriptSession =
                        exportedConsole != null ? exportedConsole.get() : scriptSessionProvider.get();
                Table table = exportedTable.get();
                scriptSession.setVariable(request.getVariableName(), table);
                if (DynamicNode.notDynamicOrIsRefreshing(table)) {
                    scriptSession.manage(table);
                }
                responseObserver.onNext(BindTableToVariableResponse.getDefaultInstance());
                responseObserver.onCompleted();
            });
        });
    }

    @Override
    public StreamObserver<AutoCompleteRequest> autoCompleteStream(
            StreamObserver<AutoCompleteResponse> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            if (AUTOCOMPLETE_DISABLED) {
                return new NoopAutoCompleteObserver(session, responseObserver);
            }
            if (PythonDeephavenSession.SCRIPT_TYPE.equals(scriptSessionProvider.get().scriptType())) {
                PyObject[] settings = new PyObject[1];
                try {
                    final ScriptSession scriptSession = scriptSessionProvider.get();
                    scriptSession.evaluateScript(
                            "from deephaven_internal.auto_completer import jedi_settings ; jedi_settings.set_scope(globals())");
                    settings[0] = (PyObject) scriptSession.getVariable("jedi_settings");
                } catch (Exception err) {
                    log.error().append("Error trying to enable jedi autocomplete").append(err).endl();
                }
                boolean canJedi = settings[0] != null && settings[0].call("can_jedi").getBooleanValue();
                log.info().append(canJedi ? "Using jedi for python autocomplete"
                        : "No jedi dependency available in python environment; disabling autocomplete.").endl();
                return canJedi ? new PythonAutoCompleteObserver(responseObserver, scriptSessionProvider, session)
                        : new NoopAutoCompleteObserver(session, responseObserver);
            }

            return new JavaAutoCompleteObserver(session, responseObserver);
        });
    }

    private static class NoopAutoCompleteObserver extends SessionCloseableObserver<AutoCompleteResponse>
            implements StreamObserver<AutoCompleteRequest> {
        public NoopAutoCompleteObserver(SessionState session, StreamObserver<AutoCompleteResponse> responseObserver) {
            super(session, responseObserver);
        }

        @Override
        public void onNext(AutoCompleteRequest value) {
            // This implementation only responds to autocomplete requests with "success, nothing found"
            if (value.getRequestCase() == AutoCompleteRequest.RequestCase.GET_COMPLETION_ITEMS) {
                safelyOnNext(responseObserver, AutoCompleteResponse.newBuilder()
                        .setCompletionItems(
                                GetCompletionItemsResponse.newBuilder().setSuccess(true)
                                        .setRequestId(value.getGetCompletionItems().getRequestId()))
                        .build());
            }
        }

        @Override
        public void onError(Throwable t) {
            // ignore, client doesn't need us, will be cleaned up later
        }

        @Override
        public void onCompleted() {
            // just hang up too, browser will reconnect if interested
            safelyComplete(responseObserver);
        }
    }

    private final class LogsClient implements LogBufferRecordListener, Runnable {
        private final RingBuffer<LogSubscriptionData> buffer;
        private final LogSubscriptionRequest request;
        private final ServerCallStreamObserver<LogSubscriptionData> client;
        private volatile boolean done = false;

        public LogsClient(
                final LogSubscriptionRequest request,
                final StreamObserver<LogSubscriptionData> client) {
            this.request = Objects.requireNonNull(request);
            this.client = (ServerCallStreamObserver<LogSubscriptionData>) Objects.requireNonNull(client);
            this.buffer = new RingBuffer<>(SUBSCRIBE_TO_LOGS_BUFFER_SIZE);
            this.client.setOnCancelHandler(this::onCancel);
            this.client.setOnCloseHandler(this::onClose);
        }

        public void start() {
            logBuffer.subscribe(this);
            scheduler.runImmediately(this);
        }

        public void stop() {
            GrpcUtil.safelyComplete(client);
        }

        @Override
        public void record(LogBufferRecord record) {
            if (done) {
                return;
            }
            // only pass levels the client wants
            if (request.getLevelsCount() != 0 && !request.getLevelsList().contains(record.getLevel().getName())) {
                return;
            }
            // since the subscribe() method auto-replays all existing logs, filter to just once this consumer probably
            // hasn't seen
            if (record.getTimestampMicros() < request.getLastSeenLogTimestamp()) {
                return;
            }
            // Note: we can't send record off-thread without doing a deepCopy.
            // io.deephaven.io.logger.LogBufferInterceptor owns io.deephaven.io.logger.LogBufferRecord.getData.
            // We can side-step this problem by creating the appropriate response here.
            final LogSubscriptionData payload = LogSubscriptionData.newBuilder()
                    .setMicros(record.getTimestampMicros())
                    .setLogLevel(record.getLevel().getName())
                    .setMessage(record.getDataString())
                    .build();
            enqueue(payload);
        }

        @Override
        public void run() {
            LogSubscriptionData payload;
            while (!done && (payload = dequeue()) != null) {
                GrpcUtil.safelyOnNext(client, payload);
            }
            if (!done) {
                scheduler.runAfterDelay(SUBSCRIBE_TO_LOGS_SEND_MILLIS, this);
            }
        }

        private void onClose() {
            done = true;
            logBuffer.unsubscribe(this);
        }

        private void onCancel() {
            done = true;
            logBuffer.unsubscribe(this);
        }

        // ------------------------------------------------------------------------------------------------------------
        // Buffer implementation
        // ------------------------------------------------------------------------------------------------------------
        // The implementation needs to support single-producer / single-consumer concurrency. The current implementation
        // uses a synchronized block around io.deephaven.base.RingBuffer. This is simple and likely sufficient. If we
        // find the implementation lacking for high volume logging, we _could_ use io.deephaven.base.LockFreeArrayQueue
        // or ArrayBlockingQueue; this would have different semantics though since they are queues, and not ring
        // buffers (we may not care about semantics for overloaded cases - if you are missing logging data, it may not
        // matter _what_ logging data you are missing).
        //
        // https://github.com/devinrsmith/JAtomicRingBuffer, or similar SPSC ring buffers, may be relevant if we need a
        // ring buffer with higher concurrent performance.

        private void enqueue(LogSubscriptionData payload) {
            synchronized (buffer) {
                buffer.addOverwrite(payload);
            }
        }

        private LogSubscriptionData dequeue() {
            synchronized (buffer) {
                return buffer.poll();
            }
        }
    }
}
