package io.deephaven.march;

public class March {

    private static MarchComponent component;

    public static synchronized void init(MarchComponent marchComponent) {
        if (component != null) {
            throw new IllegalStateException();
        }
        component = marchComponent;
    }

    public static MarchComponent get() {
        return component;
    }
}
