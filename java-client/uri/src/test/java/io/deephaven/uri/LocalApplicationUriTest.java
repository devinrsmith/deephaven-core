package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalApplicationUriTest {

    @Test
    void check1() {
        check("local:///a/appId/f/field", LocalApplicationUri.of("appId", "field"));
    }

    private static void check(String uriStr, LocalApplicationUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(LocalUri.of(uriStr)).isEqualTo(uri);
    }
}
