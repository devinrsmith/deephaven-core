package io.deephaven.plugin.application;

public interface ApplicationInfo {

    String id();

    String name();

    void initializeInto(State state);

//    Script script();
//
//    interface Script {
//        void initializeApplication(State state);
//    }

    interface State {
        <T> void setField(String name, T value);
    }
}
