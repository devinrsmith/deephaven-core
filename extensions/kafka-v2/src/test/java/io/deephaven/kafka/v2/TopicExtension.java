/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

public class TopicExtension implements ParameterResolver, BeforeEachCallback {

    // todo: do we actualyl need ParameterResolver, or just the callback part?

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER})
    public @interface Topic {
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.isAnnotated(Topic.class);
    }

    @Override
    public String resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (String.class.equals(parameterContext.getParameter().getType())) {
            return topic(extensionContext);
        }
        throw new ParameterResolutionException("No topic generator implemented for " + parameterContext);
    }

    private static String topic(ExtensionContext context) {
        return "topic-" + context.getRequiredTestMethod().getName();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        for (Field field : findAnnotatedFields(context.getRequiredTestClass(), Topic.class)) {
            field.set(context.getRequiredTestInstance(), topic(context));
        }
    }
}
