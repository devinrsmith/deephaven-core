package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.app.State;

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
        consumer.set("startTime", Instant.ofEpochMilli(runtime.getStartTime()), "The start time of the Java virtual machine.");
        if (runtime.isBootClassPathSupported()) {
            consumer.set("bootClassPath", runtime.getBootClassPath(), "The boot class path that is used by the bootstrap class loader to search for class files.");
        }
        consumer.set("classPath", runtime.getClassPath(), "The Java class path that is used by the system class loader to search for class files.");
        consumer.set("libraryPath", runtime.getLibraryPath(), "The Java library path.");
        consumer.set("vm", new VMInfo());
        consumer.set("spec", new Spec());
    }

    class VMInfo implements State {
        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("name", runtime.getVmName(), "The Java virtual machine implementation name.");
            consumer.set("vendor", runtime.getVmVendor(), "The Java virtual machine implementation vendor.");
            consumer.set("version", runtime.getVmVersion(), "The Java virtual machine implementation version.");
        }
    }

    class Spec implements State {
        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("name", runtime.getSpecName());
            consumer.set("vendor", runtime.getSpecVendor());
            consumer.set("version", runtime.getSpecVersion());
        }
    }
}
