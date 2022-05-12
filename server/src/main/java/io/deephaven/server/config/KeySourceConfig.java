package io.deephaven.server.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KeyStoreConfig.class, name = "keystore"),
        @JsonSubTypes.Type(value = PrivateKeyConfig.class, name = "privatekey"),
})
public interface KeySourceConfig {
    <V extends Visitor<T>, T> T walk(V visitor);

    interface Visitor<T> {
        T visit(KeyStoreConfig keyStore);

        T visit(PrivateKeyConfig privateKey);
    }
}
