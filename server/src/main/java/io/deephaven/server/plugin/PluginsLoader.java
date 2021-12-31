package io.deephaven.server.plugin;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;
import io.deephaven.plugin.application.ApplicationInfo;
import io.deephaven.plugin.application.ApplicationInfo.State;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.ServiceLoader;

public final class PluginsLoader implements PluginCallback {
    private static final Logger log = LoggerFactory.getLogger(PluginsLoader.class);

    private static void allServiceLoaderRegisterInto(PluginCallback callback) {
        for (Plugin provider : ServiceLoader.load(Plugin.class)) {
            provider.registerInto(callback);
        }
    }

    private static void allPythonRegisterInto(PluginCallback callback) {
        try (final PythonPluginModule pluginModule = PythonPluginModule.of()) {
            pluginModule.all_plugins_register_into(new CallbackAdapter(callback));
        }
    }

    public final ObjectTypes types;
    public final ApplicationInfos applications;

    @Inject
    public PluginsLoader(ObjectTypes types, ApplicationInfos applications) {
        this.types = Objects.requireNonNull(types);
        this.applications = Objects.requireNonNull(applications);
    }

    public void registerAll() {
        log.info().append("Registering plugins...").endl();
        final Counting serviceLoaderCount = new Counting();
        allServiceLoaderRegisterInto(serviceLoaderCount);
        final Counting pythonModuleCount = new Counting();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            allPythonRegisterInto(pythonModuleCount);
        }
        log.info().append("Registered via service loader: ").append(serviceLoaderCount).endl();
        if (ConsoleServiceGrpcImpl.isPythonSession()) {
            log.info().append("Registered via python modules: ").append(pythonModuleCount).endl();
        }
    }

    @Override
    public void registerObjectType(ObjectType objectType) {
        log.info().append("Registering object type: ")
                .append(objectType.name()).append(" / ")
                .append(objectType.toString())
                .endl();
        types.register(objectType);
    }

    @Override
    public void registerApplication(ApplicationInfo applicationInfo) {
        log.info().append("Registering application: ")
                .append(applicationInfo.name()).append(" / ")
                .append(applicationInfo.id()).append(" / ")
                .append(applicationInfo.toString())
                .endl();
        applications.register(applicationInfo);
    }

    private class Counting implements PluginCallback, LogOutputAppendable {

        private int objectTypeCount = 0;
        private int applicationCount = 0;

        @Override
        public void registerObjectType(ObjectType objectType) {
            PluginsLoader.this.registerObjectType(objectType);
            ++objectTypeCount;
        }

        @Override
        public void registerApplication(ApplicationInfo applicationInfo) {
            PluginsLoader.this.registerApplication(applicationInfo);
            ++applicationCount;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput
                    .append("objectType=").append(objectTypeCount)
                    .append(",application=").append(applicationCount);
        }
    }

    interface PythonCustomType {

        static PythonCustomType of(PyObject object) {
            return (PythonCustomType) object.createProxy(CallableKind.FUNCTION, PythonCustomType.class);
        }

        String name();

        boolean is_type(PyObject object);

        // TODO(deephaven-core#1785): Use more pythonic wrapper for io.deephaven.plugin.type.Exporter
        byte[] to_bytes(Exporter exporter, PyObject object);
    }


//    interface PythonApplication {
//
//        static PythonApplication of(PyObject object) {
//            return (PythonApplication) object.createProxy(CallableKind.FUNCTION, PythonApplication.class);
//        }
//
//        String id();
//
//        String name();
//
//        void initialize_application(PyObject state);
//    }

    private static final class Adapter extends ObjectTypeBase {

        public static Adapter of(PythonCustomType module) {
            return new Adapter(module.name(), module);
        }

        private final String name;
        private final PythonCustomType module;

        private Adapter(String name, PythonCustomType module) {
            this.name = Objects.requireNonNull(name);
            this.module = Objects.requireNonNull(module);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isType(Object o) {
            if (!(o instanceof PyObject)) {
                return false;
            }
            return module.is_type((PyObject) o);
        }

        @Override
        public void writeToTypeChecked(Exporter exporter, Object object, OutputStream out) throws IOException {
            out.write(module.to_bytes(exporter, (PyObject) object));
        }

        @Override
        public String toString() {
            return name + ":" + module;
        }
    }

    private static class CallbackAdapter {
        private final PluginCallback callback;

        public CallbackAdapter(PluginCallback callback) {
            this.callback = Objects.requireNonNull(callback);
        }

        @SuppressWarnings("unused")
        public void register_object_type(PyObject module) {
            final PythonCustomType pythonCustomType = PythonCustomType.of(module);
            final Adapter adapter = Adapter.of(pythonCustomType);
            callback.registerObjectType(adapter);
        }

        // Remove before merge, but still want code reviewers to be able to use existing matplotlib plugin
        @Deprecated
        public void register_custom_type(PyObject module) {
            register_object_type(module);
        }

        @SuppressWarnings("unused")
        public void register_application(PyObject module) {

            try (final PyModule m = PyModule.importModule("deephaven.server.app_state")) {
                final ApplicationInfo proxy = (ApplicationInfo) m.call("ApplicationAdapter", module)
                        .createProxy(CallableKind.FUNCTION, ApplicationInfo.class);
                callback.registerApplication(proxy);
            }

//            final PythonApplication pythonApplication = PythonApplication.of(module);
//            callback.registerApplication(new ApplicationAdapter(pythonApplication));
        }


//        private static class ApplicationAdapter implements ApplicationInfo, Script {
//            private final PythonApplication pythonApplication;
//
//            public ApplicationAdapter(PythonApplication pythonApplication) {
//                this.pythonApplication = Objects.requireNonNull(pythonApplication);
//            }
//
//            @Override
//            public String id() {
//                return pythonApplication.id();
//            }
//
//            @Override
//            public String name() {
//                return pythonApplication.name();
//            }
//
//            @Override
//            public void initializeInto(State state) {
////                try (final PythonScriptSessionModule module = PythonScriptSessionModule.of()) {
////                    pythonApplication.initialize_application(new PythonApplicationState(state, module));
////                }
//                try (
//                        final PyModule module = PyModule.importModule("deephaven.server.app_state");
//                        final PyObject wrapped = module.call("ApplicationState", new PythonApplicationState(state))) {
//                    pythonApplication.initialize_application(wrapped);
//                }
//            }
//
//            @Override
//            public String toString() {
//                return pythonApplication.toString();
//            }
//        }
    }

    interface PythonPluginModule extends AutoCloseable {

        static PythonPluginModule of() {
            return (PythonPluginModule) PyModule.importModule("deephaven.plugin")
                    .createProxy(CallableKind.FUNCTION, PythonPluginModule.class);
        }

        void all_plugins_register_into(CallbackAdapter callback);

        @Override
        void close();
    }
}
