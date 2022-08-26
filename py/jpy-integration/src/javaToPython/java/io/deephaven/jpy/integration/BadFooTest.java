package io.deephaven.jpy.integration;

import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BadFooTest extends PythonTest {
    @Test
    public void badFooScript() {
        PyObject.executeCode(readResource("badfoo.py"), PyInputMode.SCRIPT);
        try {
            PyObject.executeCode("BadFoo()", PyInputMode.SCRIPT);
        } catch (RuntimeException e) {
            if (!e.getMessage().contains("I hate fun")) {
                throw e;
            }
        }
    }

    private static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(PyLibTest.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
