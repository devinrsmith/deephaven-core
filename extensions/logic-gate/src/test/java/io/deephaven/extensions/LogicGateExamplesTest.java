package io.deephaven.extensions;


import io.deephaven.logicgate.LogicGateResources;
import java.io.IOException;
import java.util.Iterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogicGateExamplesTest {

    @Test
    void isRegistered() {
        Assertions.assertTrue(ResourceProvider.test().map(ResourceProvider::getClass)
            .anyMatch(LogicGateResources.class::equals));
    }

    @Test
    void contentBytes() throws IOException {
        Iterator<Resource> it = new LogicGateResources().getResources().iterator();
        while (it.hasNext()) {
            it.next().contentBytes();
        }
    }
}
