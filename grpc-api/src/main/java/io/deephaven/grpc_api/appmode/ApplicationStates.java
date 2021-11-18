package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;

import java.util.Optional;
import java.util.function.BiConsumer;

public interface ApplicationStates {

    Optional<ApplicationState> getApplicationState(String applicationId);

    void forEach(BiConsumer<ApplicationState, Field<?>> consumer);
}
