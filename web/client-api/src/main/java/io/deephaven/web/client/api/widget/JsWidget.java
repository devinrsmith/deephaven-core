/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.ArrayBuffer;
import elemental2.core.DataView;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

@TsName(namespace = "dh", name = "Widget")
public class JsWidget extends HasEventHandling {
    @JsProperty(namespace = "dh.Widget")
    public static final String EVENT_MESSAGE = "message";
    private final WorkerConnection connection;
    private final TypedTicket typedTicket;

    private boolean hasFetched;

    private final Supplier<BiDiStream<StreamRequest, StreamResponse>> streamFactory;
    private BiDiStream<StreamRequest, StreamResponse> messageStream;

    private StreamResponse response;

    private JsArray<JsWidgetExportedObject> exportedObjects;

    public JsWidget(WorkerConnection connection, TypedTicket typedTicket) {
        this.connection = connection;
        this.typedTicket = typedTicket;
        hasFetched = false;
        BiDiStream.Factory<StreamRequest, StreamResponse> factory = connection.streamFactory();
        streamFactory = () -> factory.create(
                connection.objectServiceClient()::messageStream,
                (first, headers) -> connection.objectServiceClient().openMessageStream(first, headers),
                (next, headers, c) -> connection.objectServiceClient().nextMessageStream(next, headers, c::apply),
                new StreamRequest());
        this.exportedObjects = new JsArray<>();
    }

    private void closeStream() {
        if (messageStream != null) {
            messageStream.end();
            messageStream = null;
        }
        hasFetched = false;
    }

    public Promise<JsWidget> refetch() {
        closeStream();
        return new Promise<>((resolve, reject) -> {
            exportedObjects = new JsArray<>();

            messageStream = streamFactory.get();
            messageStream.onData(res -> {
                //TODO only assign to fields for the first one
                response = res;
                exportedObjects = res.getData().getTypedExportIdsList().map((p0, p1, p2) -> new JsWidgetExportedObject(connection, p0));

                if (!hasFetched) {
                    hasFetched = true;
                    resolve.onInvoke(this);
                    return;
                }

                CustomEventInit<JsWidgetMessageWrapper> messageEvent = CustomEventInit.create();
                messageEvent.setDetail(new JsWidgetMessageWrapper(res, exportedObjects));
                fireEvent(EVENT_MESSAGE, messageEvent);
            });
            messageStream.onStatus(status -> {
                if (!status.isOk()) {
                    reject.onInvoke(status.getDetails());
                    closeStream();
                }
            });
            messageStream.onEnd(status -> {
                closeStream();
            });

            // First message establishes a connection w/ the plugin object instance we're talking to
            StreamRequest req = new StreamRequest();
            ConnectRequest data = new ConnectRequest();
            data.setTypedTicket(typedTicket);
            req.setConnect(data);
            messageStream.send(req);
        });
    }

    public Ticket getTicket() {
        return typedTicket.getTicket();
    }

    @JsProperty
    public String getType() {
        return typedTicket.getType();
    }

    @JsMethod
    public String getDataAsBase64() {
        return response.getData().getPayload_asB64();
    }

    @JsMethod
    public Uint8Array getDataAsU8() {
        return response.getData().getPayload_asU8();
    }

    @JsMethod
    public String getDataAsString() {
        return new String(Js.<byte[]>uncheckedCast(response.getData().getPayload_asU8()), StandardCharsets.UTF_8);
    }

    @JsProperty
    public JsWidgetExportedObject[] getExportedObjects() {
        return Js.<JsWidgetExportedObject[]>uncheckedCast(exportedObjects);
    }

    @JsMethod
    public JsWidgetExportedObject getExportedObject(int index) {
        return exportedObjects.getAt(index);
    }

    @JsMethod
    public void sendMessage(Object msg) {
        if (messageStream == null) {
            return;
        }
        StreamRequest req = new StreamRequest();
        Data data = new Data();
        if (msg instanceof String) {
            byte[] bytes = ((String) msg).getBytes(StandardCharsets.UTF_8);
            Uint8Array payload = new Uint8Array(bytes.length);
            payload.set(Js.<double[]>uncheckedCast(bytes));
            data.setPayload(payload);
        } else if (msg instanceof ArrayBuffer) {
            data.setPayload(new Uint8Array((ArrayBuffer) msg));
        } else if (ArrayBuffer.isView(msg)) {
            // can cast (unsafely) to any typed array or to DataView to read offset/length/buffer to make a new view
            DataView view = Js.uncheckedCast(msg);
            data.setPayload(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
        } else {
            throw new IllegalArgumentException("Expected message to be a String or ArrayBuffer");
        }

        req.setData(data);
        messageStream.send(req);
    }

    private static class JsWidgetMessageWrapper {
        private final StreamResponse message;

        private final JsArray<JsWidgetExportedObject> exportedObjects;

        public JsWidgetMessageWrapper(StreamResponse m, JsArray<JsWidgetExportedObject> e) {
            message = m;
            exportedObjects = e;
        }

        @JsMethod
        public String getDataAsBase64() {
            return message.getData().getPayload_asB64();
        }

        @JsMethod
        public Uint8Array getDataAsU8() {
            return message.getData().getPayload_asU8();
        }

        @JsMethod
        public String getDataAsString() {
            return new String(Js.<byte[]>uncheckedCast(message.getData().getPayload_asU8()), StandardCharsets.UTF_8);
        }

        @JsProperty
        public JsWidgetExportedObject[] getExportedObjects() {
            return Js.<JsWidgetExportedObject[]>uncheckedCast(exportedObjects);
        }

        @JsMethod
        public JsWidgetExportedObject getExportedObject(int index) {
            return exportedObjects.getAt(index);
        }
    }
}
