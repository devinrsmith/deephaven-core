package io.deephaven.server.jetty;

import java.util.LinkedHashSet;
import java.util.Set;

public class JvmSpecificRecommendations {
    public static void main(String[] args) {
        final Set<String> jvmArguments = new LinkedHashSet<>();
        if (Jdk8287432.hasBug()) {
            jvmArguments.addAll(Jdk8287432.workaroundJvmArguments());
        }
        for (String arg : jvmArguments) {
            System.out.println(arg);
        }
    }
}
