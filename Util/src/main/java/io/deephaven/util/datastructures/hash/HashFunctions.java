package io.deephaven.util.datastructures.hash;

class HashFunctions {

//    // https://github.com/skeeto/hash-prospector/issues/19
//    public static int hash(int x) {
//        x ^= x >>> 16;
//        x *= 0x21f0aaad;
//        x ^= x >>> 15;
//        x *= 0xd35a2d97;
//        x ^= x >>> 15;
//        return x;
//    }
//
//    public static int hash(long b) {
//        int x = (int)b ^ (int)(b >> 32);
//        x ^= x >>> 16;
//        x *= 0x21f0aaad;
//        x ^= x >>> 15;
//        x *= 0xd35a2d97;
//        x ^= x >>> 15;
//        return x;
//    }

//    /**
//     * Returns the 32 high bits of Stafford variant 4 mix64 function as int.
//     *
//     * See java.util.SplittableRandom#mix32(long).
//     */
//    static int mix32(long z) {
//        z = (z ^ (z >>> 33)) * 0x62a9d9ed799705f5L;
//        return (int)(((z ^ (z >>> 28)) * 0xcb24d0a5c88c35b3L) >>> 32);
//    }

    // http://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
    public static long rrxmrrxmsx_0(long v) {
        v ^= Long.rotateRight(v, 25) ^ Long.rotateRight(v, 50);
        v *= 0xA24BAED4963EE407L;
        v ^= Long.rotateRight(v, 24) ^ Long.rotateRight(v, 49);
        v *= 0x9FB21C651E98DF25L;
        return v ^ v >>> 28;
    }

    // http://mostlymangling.blogspot.com/2020/01/nasam-not-another-strange-acronym-mixer.html
    /*
    uint64_t nasam(uint64_t x) {
        x ^= ror64(x, 25) ^ ror64(x, 47);
      x *= 0x9E6C63D0676A9A99UL;
      x ^= x >> 23 ^ x >> 51;
      x *= 0x9E6D62D06F6A9A9BUL;
      x ^= x >> 23 ^ x >> 51;
      return x;
    }
     */

    // http://mostlymangling.blogspot.com/2019/12/stronger-better-morer-moremur-better.html
    public static long moremur(long x) {
        x ^= x >>> 27;
        x *= 0x3C79AC492BA7B653L;
        x ^= x >>> 33;
        x *= 0x1C69B3F74AC4AE35L;
        x ^= x >>> 27;
        return x;
    }

    public static int fold32(long x, int bits) {
        final int mask = (1 << bits) - 1;
        return (int)((x >>> bits) ^ (x & mask));
    }

    /**
     * @see <a href="http://mostlymangling.blogspot.com/2019/12/stronger-better-morer-moremur-better.html">moremur</a>
     * @param x the input
     * @return the hashed output
     */
    public static int medium_mix(long x) {
//        return gnu.trove.impl.HashFunctions.hash(x);
        x ^= x >>> 27;
        x *= 0x3C79AC492BA7B653L;
        x ^= x >>> 33;
        x *= 0x1C69B3F74AC4AE35L;
        x ^= x >>> 27;
        return (int)x;
    }

//    public static long hash64(long val) {
//        // http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
//        long hash = 0xcbf29ce484222325L; // 14695981039346656037
//        for (int i = 0; i < 8; ++i) {
//            hash ^= (byte) (val >> (8 * (7 - i)));
//            hash *= 1099511628211L;
//        }
//        return hash;
//    }

//    public static int hash32ish(long val, int bits) {
//        final long mix = hash64(val);
//        return (int)((mix >>> bits) ^ (mix & (1 << bits - 1)));
//    }

    public static long mix(long x) {
        return moremur(x);
    }

    public static int hash32(long x) {
        x ^= x >>> 27;
        x *= 0x3C79AC492BA7B653L;
        x ^= x >>> 33;
        x *= 0x1C69B3F74AC4AE35L;
        x ^= x >>> 27;
        return (int)x;
    }
}
