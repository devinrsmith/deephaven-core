package io.deephaven.util.datastructures.hash;

public class HashFunctionsForBenchmarking {

    /**
     * <a href="http://mostlymangling.blogspot.com/2019/12/stronger-better-morer-moremur-better.html">moremur</a>
     */
    public static int moremur(long x) {
        x ^= x >>> 27;
        x *= 0x3C79AC492BA7B653L;
        x ^= x >>> 33;
        x *= 0x1C69B3F74AC4AE35L;
        x ^= x >>> 27;
        return (int)x;
    }
}
