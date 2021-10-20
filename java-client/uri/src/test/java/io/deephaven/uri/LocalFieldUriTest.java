package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalFieldUriTest {

    @Test
    void check1() {
        check("local:///f/field", LocalFieldUri.of("field"));
    }

    private static void check(String uriStr, LocalFieldUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(LocalUri.of(uriStr)).isEqualTo(uri);
    }
}
