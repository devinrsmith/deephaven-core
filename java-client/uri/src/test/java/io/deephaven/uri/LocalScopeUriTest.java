package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalScopeUriTest {

    @Test
    void check1() {
        check("local:///s/my_table", LocalQueryScopeUri.of("my_table"));
    }

    private static void check(String uriStr, LocalQueryScopeUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(LocalUri.of(uriStr)).isEqualTo(uri);
    }
}
