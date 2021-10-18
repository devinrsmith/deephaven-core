package io.deephaven;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DeephavenUriITest {
    @Test
    void applicationField() {
        check("dh://host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .applicationId("appId")
                .fieldName("field")
                .build());
    }

    @Test
    void field() {
        check("dh://host/f/field", DeephavenUriField.builder()
                .host("host")
                .fieldName("field")
                .build());
    }

    @Test
    void queryScope() {
        check("dh://host/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .variableName("variable")
                .build());
    }

    @Test
    void applicationFieldPort() {
        check("dh://host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .port(31337)
                .applicationId("appId")
                .fieldName("field")
                .build());
    }

    @Test
    void fieldPort() {
        check("dh://host:31337/f/field", DeephavenUriField.builder()
                .host("host")
                .port(31337)
                .fieldName("field")
                .build());
    }

    @Test
    void queryScopePort() {
        check("dh://host:31337/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .port(31337)
                .variableName("variable")
                .build());
    }

    @Test
    void proxyApplicationField() {
        check("dh://gateway/h/host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyField() {
        check("dh://gateway/h/host/f/field", DeephavenUriField.builder()
                .host("host")
                .fieldName("field")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyQueryScope() {
        check("dh://gateway/h/host/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .variableName("variable")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyApplicationFieldPort() {
        check("dh://gateway/h/host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .port(31337)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyFieldPort() {
        check("dh://gateway/h/host:31337/f/field", DeephavenUriField.builder()
                .host("host")
                .port(31337)
                .fieldName("field")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyQueryScopePort() {
        check("dh://gateway/h/host:31337/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .port(31337)
                .variableName("variable")
                .build()
                .proxyVia("gateway"));
    }

    @Test
    void proxyPortApplicationField() {
        check("dh://gateway:42/h/host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void proxyPortField() {
        check("dh://gateway:42/h/host/f/field", DeephavenUriField.builder()
                .host("host")
                .fieldName("field")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void proxyPortQueryScope() {
        check("dh://gateway:42/h/host/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .variableName("variable")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void proxyPortApplicationFieldPort() {
        check("dh://gateway:42/h/host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .host("host")
                .port(31337)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void proxyPortFieldPort() {
        check("dh://gateway:42/h/host:31337/f/field", DeephavenUriField.builder()
                .host("host")
                .port(31337)
                .fieldName("field")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void proxyPortQueryScopePort() {
        check("dh://gateway:42/h/host:31337/s/variable", DeephavenUriQueryScope.builder()
                .host("host")
                .port(31337)
                .variableName("variable")
                .build()
                .proxyVia("gateway", 42));
    }

    @Test
    void doubleProxy() {
        check("dh://gateway-1/h/gateway-2/h/host/f/field", DeephavenUriField.builder()
                .host("host")
                .fieldName("field")
                .build()
                .proxyVia("gateway-2")
                .proxyVia("gateway-1"));
    }

    static void check(String uriString, DeephavenUriI uri) {
        assertThat(uri.toString()).isEqualTo(uriString);
        assertThat(DeephavenUriI.from(uriString)).isEqualTo(uri);
    }
}
