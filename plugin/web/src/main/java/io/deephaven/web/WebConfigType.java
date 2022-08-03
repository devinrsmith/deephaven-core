package io.deephaven.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;

import java.io.IOException;
import java.io.OutputStream;

@AutoService(ObjectType.class)
public final class WebConfigType extends ObjectTypeBase {
    private static final ObjectWriter writer;

    static {
        final ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);
        writer = om.writerFor(WebConfig.class);
    }

    @Override
    public String name() {
        return WebConfig.class.getName();
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof WebConfig;
    }

    @Override
    public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        writer.writeValue(out, object);
    }
}
