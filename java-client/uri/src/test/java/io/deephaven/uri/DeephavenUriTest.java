package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DeephavenUriTest {

    private static final DeephavenTarget TARGET = DeephavenTarget.builder()
            .isTLS(true)
            .host("host")
            .build();

    private static final DeephavenTarget TARGET_PORT = DeephavenTarget.builder()
            .isTLS(true)
            .host("host")
            .port(31337)
            .build();

    private static final DeephavenTarget GATEWAY = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway")
            .build();

    private static final DeephavenTarget GATEWAY_PORT = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway")
            .port(42)
            .build();

    private static final DeephavenTarget GATEWAY_1 = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway-1")
            .build();

    private static final DeephavenTarget GATEWAY_2 = DeephavenTarget.builder()
            .isTLS(false)
            .host("gateway-2")
            .build();

    @Test
    void applicationField() {
        check("dh://host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET)
                .applicationId("appId")
                .fieldName("field")
                .build());
    }

    @Test
    void field() {
        check("dh://host/f/field", DeephavenUriField.builder()
                .target(TARGET)
                .fieldName("field")
                .build());
    }

    @Test
    void queryScope() {
        check("dh://host/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET)
                .variableName("variable")
                .build());
    }

    @Test
    void applicationFieldPort() {
        check("dh://host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET_PORT)
                .applicationId("appId")
                .fieldName("field")
                .build());
    }

    @Test
    void fieldPort() {
        check("dh://host:31337/f/field", DeephavenUriField.builder()
                .target(TARGET_PORT)
                .fieldName("field")
                .build());
    }

    @Test
    void queryScopePort() {
        check("dh://host:31337/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET_PORT)
                .variableName("variable")
                .build());
    }

    @Test
    void proxyApplicationField() {
        check("dh://gateway/dh/host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyField() {
        check("dh://gateway/dh/host/f/field", DeephavenUriField.builder()
                .target(TARGET)
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyQueryScope() {
        check("dh://gateway/dh/host/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET)
                .variableName("variable")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyApplicationFieldPort() {
        check("dh://gateway/dh/host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET_PORT)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyFieldPort() {
        check("dh://gateway/dh/host:31337/f/field", DeephavenUriField.builder()
                .target(TARGET_PORT)
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyQueryScopePort() {
        check("dh://gateway/dh/host:31337/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET_PORT)
                .variableName("variable")
                .build()
                .proxyVia(GATEWAY));
    }

    @Test
    void proxyPortApplicationField() {
        check("dh://gateway:42/dh/host/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void proxyPortField() {
        check("dh://gateway:42/dh/host/f/field", DeephavenUriField.builder()
                .target(TARGET)
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void proxyPortQueryScope() {
        check("dh://gateway:42/dh/host/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET)
                .variableName("variable")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void proxyPortApplicationFieldPort() {
        check("dh://gateway:42/dh/host:31337/a/appId/f/field", DeephavenUriApplicationField.builder()
                .target(TARGET_PORT)
                .applicationId("appId")
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void proxyPortFieldPort() {
        check("dh://gateway:42/dh/host:31337/f/field", DeephavenUriField.builder()
                .target(TARGET_PORT)
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void proxyPortQueryScopePort() {
        check("dh://gateway:42/dh/host:31337/s/variable", DeephavenUriQueryScope.builder()
                .target(TARGET_PORT)
                .variableName("variable")
                .build()
                .proxyVia(GATEWAY_PORT));
    }

    @Test
    void doubleProxy() {
        check("dh://gateway-1/dh-plain/gateway-2/dh/host/f/field", DeephavenUriField.builder()
                .target(TARGET)
                .fieldName("field")
                .build()
                .proxyVia(GATEWAY_2)
                .proxyVia(GATEWAY_1));
    }

    static void check(String uriString, DeephavenUri uri) {
        assertThat(uri.toString()).isEqualTo(uriString);
        assertThat(DeephavenUri.of(uriString)).isEqualTo(uri);
    }
}
