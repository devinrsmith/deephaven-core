package io.deephaven.server.python;

import dagger.Module;
import io.deephaven.server.plugin.python.PythonPluginRegistration;

@Module(includes = PythonPluginRegistration.Module.class)
public interface PythonModule {

}
