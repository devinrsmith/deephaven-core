package io.deephaven.vector;

import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.Type;

import java.lang.reflect.Proxy;

public interface BooleanVector extends ObjectVector<Boolean> {

    static PrimitiveVectorType<BooleanVector, Boolean> type() {
        return PrimitiveVectorType.of(BooleanVector.class, Type.booleanType());
    }

    static BooleanVector proxy(ObjectVector<Boolean> delegate) {
        if (delegate instanceof BooleanVector) {
            return (BooleanVector) delegate;
        }
        return (BooleanVector) Proxy.newProxyInstance(
                BooleanVector.class.getClassLoader(),
                new Class[] {BooleanVector.class},
                (proxy, method, args) -> method.invoke(delegate, args));
    }

    static BooleanVector empty() {
        return proxy(ObjectVectorDirect.empty());
    }
}
