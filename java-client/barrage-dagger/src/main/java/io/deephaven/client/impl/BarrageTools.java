package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;

public class BarrageTools {

    private static final BarrageSessions sessions = BarrageSessions.tmp();

    public static Table subscribe(String deephavenUri) throws TableHandleException, InterruptedException {
        return sessions.subscribe(deephavenUri);
    }
}
