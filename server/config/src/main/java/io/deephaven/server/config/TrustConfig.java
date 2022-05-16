package io.deephaven.server.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TrustStoreConfig.class, name = "truststore"),
        @JsonSubTypes.Type(value = TrustStoreConfig.class, name = "certs"),
})
public interface TrustConfig {
    <V extends Visitor<T>, T> T walk(V visitor);

    interface Visitor<T> {
        T visit(TrustStoreConfig trustStore);

        T visit(TrustCertificatesConfig certificates);
    }
}
