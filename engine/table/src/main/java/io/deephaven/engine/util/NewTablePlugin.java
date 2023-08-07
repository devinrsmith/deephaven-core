package io.deephaven.engine.util;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@AutoService(ObjectType.class)
public class NewTablePlugin extends ObjectTypeBase {

    public static class Config {
        private final String columnName;

        public Config(String columnName) {
            this.columnName = Objects.requireNonNull(columnName);
        }
    }

    public NewTablePlugin() {}

    @Override
    public String name() {
        return NewTablePlugin.class.getSimpleName();
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof Config;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection) {
        return new Impl((Config) object, connection);
    }

    private static class Impl implements MessageStream {
        private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

        private final Config config;
        private final MessageStream client;

        public Impl(Config config, MessageStream client) {
            this.config = Objects.requireNonNull(config);
            this.client = Objects.requireNonNull(client);;
        }

        @Override
        public void onData(ByteBuffer payload, Object... references) throws ObjectCommunicationException {
            if (references.length != 0) {
                throw new ObjectCommunicationException("Expected zero references");
            }
            final byte[] dst = new byte[payload.remaining()];
            payload.get(dst);
            final int numTables;
            try {
                numTables = Integer.parseInt(new String(dst, StandardCharsets.UTF_8).trim());
            } catch (NumberFormatException e) {
                throw new ObjectCommunicationException(e);
            }
            if (numTables < 0) {
                throw new ObjectCommunicationException("Expected non-negative int payload");
            }
            final Table[] results = new Table[numTables];
            for (int i = 0; i < numTables; i++) {
                results[i] = TableTools.newTable(TableTools.intCol(config.columnName, i));
            }
            client.onData(EMPTY_BUFFER, (Object[]) results);
        }

        @Override
        public void onClose() {
            // ignore
        }
    }
}
