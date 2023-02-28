package io.deephaven.benchmark;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.jetty.CommunityComponentFactory;
import io.deephaven.server.runner.MainHelper;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class BenchmarkMain {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration configuration = MainHelper.init(args, BenchmarkMain.class);
        new CommunityComponentFactory()
                .build(configuration)
                .getServer()
                .run()
                .join();
    }
}
