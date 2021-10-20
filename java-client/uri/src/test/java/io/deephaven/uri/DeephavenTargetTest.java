package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DeephavenTargetTest {
    @Test
    void tlsLocalhost() {
        check("dh://localhost", DeephavenTarget.builder().isTLS(true).host("localhost").build());
    }

    @Test
    void plaintextLocalhost() {
        check("dh-plain://localhost", DeephavenTarget.builder().isTLS(false).host("localhost").build());
    }

    @Test
    void tlsLocalhost42() {
        check("dh://localhost:42", DeephavenTarget.builder().isTLS(true).host("localhost").port(42).build());
    }

    @Test
    void plaintextLocalhost42() {
        check("dh-plain://localhost:42", DeephavenTarget.builder().isTLS(false).host("localhost").port(42).build());
    }

    @Test
    void noScheme() {
        try {
            DeephavenTarget.of("localhost");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void queryParams() {
        try {
            DeephavenTarget.of("dh://localhost?bad=1");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void fragment() {
        try {
            DeephavenTarget.of("dh://localhost#bad");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void noHost() {
        try {
            DeephavenTarget.of("dh://");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void noHost2() {
        try {
            DeephavenTarget.of("dh:///");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasSlash() {
        try {
            DeephavenTarget.of("dh://localhost/");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasPath() {
        try {
            DeephavenTarget.of("dh://localhost/s/table");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasUserInfo() {
        try {
            DeephavenTarget.of("dh://user@localhost");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void check(String uri, DeephavenTarget target) {
        assertThat(target.toString()).isEqualTo(uri);
        assertThat(DeephavenTarget.of(uri)).isEqualTo(target);
    }
}
