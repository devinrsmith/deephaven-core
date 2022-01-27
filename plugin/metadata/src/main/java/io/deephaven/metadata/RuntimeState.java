package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.plugin.app.State;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Instant;
import java.util.Objects;

@AutoService(State.class)
public class RuntimeState implements State {

    private final RuntimeMXBean runtime;

    public RuntimeState() {
        this(ManagementFactory.getRuntimeMXBean());
    }

    public RuntimeState(RuntimeMXBean runtime) {
        this.runtime = Objects.requireNonNull(runtime);
    }

    @Override
    public void insertInto(Consumer consumer) {
        // todo
        consumer.set("pid", runtime.getPid());
        consumer.set("startTime", Instant.ofEpochMilli(runtime.getStartTime()), "The start time of the JVM.");
        if (runtime.isBootClassPathSupported()) {
            consumer.set("bootClassPath", runtime.getBootClassPath(), "The boot class path that is used by the bootstrap class loader to search for class files.");
        }
        consumer.set("classPath", runtime.getClassPath(), "The Java class path that is used by the system class loader to search for class files.");
        consumer.set("libraryPath", runtime.getLibraryPath(), "The Java library path.");
        consumer.set("vm", new VMInfo());
        consumer.set("spec", new Spec());
        consumer.set("inputArguments", runtime.getInputArguments().toArray(String[]::new), "The input arguments passed to the JVM.");

        final LongSingleValue uptimeTable = LongSingleValue.create();
        consumer.set("uptimeTable", uptimeTable.table());

        new Thread(() -> {
            while (true) {
                // hacky
                final long uptime = runtime.getUptime();
                consumer.set("uptime", uptime);
                try {
                    uptimeTable.set(uptime);
                } catch (IOException e) {
                    return;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }).start();
    }

    class VMInfo implements State {
        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("name", runtime.getVmName(), "The JVM implementation name.");
            consumer.set("vendor", runtime.getVmVendor(), "The JVM implementation vendor.");
            consumer.set("version", runtime.getVmVersion(), "The JVM implementation version.");
        }
    }

    class Spec implements State {
        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("name", runtime.getSpecName(), "The JVM specification name.");
            consumer.set("vendor", runtime.getSpecVendor(), "The JVM specification vendor.");
            consumer.set("version", runtime.getSpecVersion(), "The JVM specification version.");
        }
    }

    private static class LongSingleValue {


        public static final ColumnHeader<Long> HEADER = ColumnHeader.ofLong("Value");

        public static LongSingleValue create() {
            final KeyedArrayBackedMutableTable keyed = KeyedArrayBackedMutableTable.make(TableDefinition.from(HEADER));
            return new LongSingleValue(keyed.mutableInputTable(), keyed.readOnlyCopy());
        }

        private final MutableInputTable inputTable;
        private final Table readOnlyTable;

        public LongSingleValue(MutableInputTable inputTable, Table readOnlyTable) {
            this.inputTable = Objects.requireNonNull(inputTable);
            this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
        }

        public Table table() {
            return readOnlyTable;
        }

        public void set(Long value) throws IOException {
            final InMemoryTable entry = InMemoryTable.from(HEADER.start(1).row(value).newTable());
            inputTable.add(entry);
        }

        public void clear() throws IOException {
            final InMemoryTable entry = InMemoryTable.from(NewTable.empty(TableHeader.empty()));
            inputTable.delete(entry);
        }
    }
}
