package io.deephaven.server.jetty;

import java.lang.Runtime.Version;
import java.util.List;

/**
 * See <a href="https://bugs.openjdk.org/browse/JDK-8287432">JDK-8287432</a>.
 */
public class Jdk8287432 {
    private static final Version FIX_V11 = Version.parse("11.0.17");
    private static final Version FIX_V17 = Version.parse("17.0.5");
    private static final List<String> WORKAROUND =
            List.of("-XX:+UnlockDiagnosticVMOptions", "-XX:DisableIntrinsic=_currentThread");

    public static boolean hasBug(Version version) {
        switch (version.feature()) {
            case 11:
                return version.compareTo(FIX_V11) < 0;
            case 17:
                return version.compareTo(FIX_V17) < 0;
        }
        // Assume the version does not have the bug.
        return false;
    }

    public static boolean hasBug() {
        return hasBug(Runtime.version());
    }

    public static List<String> workaroundJvmArguments() {
        return WORKAROUND;
    }
}
