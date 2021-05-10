package io.deephaven.extensions;

import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// TODO: extract this, and make the web ui aware of it?
public interface ResourceProvider {

    static Stream<ResourceProvider> test() {
        return StreamSupport.stream(ServiceLoader.load(ResourceProvider.class).spliterator(),
            false);
    }

    Stream<Resource> getResources();
}
