/**
 * Ported from Guava
 */

package com.hazelcast.nio.serialization;

import java.nio.ByteOrder;
import java.util.Comparator;

import sun.misc.Unsafe;

public enum UnsafeComparator implements Comparator<byte[]> {
    INSTANCE;

    private static final int UNSIGNED_MASK = 0xFF;

    private static final int LONG_BYTES = 8;

    static final boolean littleEndian = ByteOrder.nativeOrder().equals(
            ByteOrder.LITTLE_ENDIAN);

    /*
     * The following static final fields exist for performance reasons.
     * 
     * In UnsignedBytesBenchmark, accessing the following objects via static
     * final fields is the fastest (more than twice as fast as the Java
     * implementation, vs ~1.5x with non-final static fields, on x86_32) under
     * the Hotspot server compiler. The reason is obviously that the non-final
     * fields need to be reloaded inside the loop.
     * 
     * And, no, defining (final or not) local variables out of the loop still
     * isn't as good because the null check on the theUnsafe object remains
     * inside the loop and BYTE_ARRAY_BASE_OFFSET doesn't get constant-folded.
     * 
     * The compiler can treat static final fields as compile-time constants and
     * can constant-fold them while (final or not) local variables are run time
     * values.
     */

    static final Unsafe theUnsafe;

    /** The offset to the first element in a byte array. */
    static final int BYTE_ARRAY_BASE_OFFSET;

    static {
        theUnsafe = getUnsafe();

        BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
            throw new AssertionError();
        }
    }

    /**
     * Returns a sun.misc.Unsafe. Suitable for use in a 3rd party package.
     * Replace with a simple call to Unsafe.getUnsafe when integrating into a
     * jdk.
     *
     * @return a sun.misc.Unsafe
     */
    private static sun.misc.Unsafe getUnsafe() {
        try {
            return sun.misc.Unsafe.getUnsafe();
        } catch (SecurityException tryReflectionInstead) {
        }
        try {
            return java.security.AccessController
                    .doPrivileged(new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                        public sun.misc.Unsafe run() throws Exception {
                            Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                            for (java.lang.reflect.Field f : k
                                    .getDeclaredFields()) {
                                f.setAccessible(true);
                                Object x = f.get(null);
                                if (k.isInstance(x))
                                    return k.cast(x);
                            }
                            throw new NoSuchFieldError("the Unsafe");
                        }
                    });
        } catch (java.security.PrivilegedActionException e) {
            throw new RuntimeException("Could not initialize intrinsics",
                    e.getCause());
        }
    }

    private static long flip(long a) {
        return a ^ Long.MIN_VALUE;
    }

    /**
     * Compares the two specified {@code long} values, treating them as unsigned
     * values between {@code 0} and {@code 2^64 - 1} inclusive.
     *
     * @param a
     *            the first unsigned {@code long} to compare
     * @param b
     *            the second unsigned {@code long} to compare
     * @return a negative value if {@code a} is less than {@code b}; a positive
     *         value if {@code a} is greater than {@code b}; or zero if they are
     *         equal
     */
    public static int compareFlipped(long a, long b) {
        return compare(flip(a), flip(b));
    }

    public static int compare(long a, long b) {
        return (a < b) ? -1 : ((a > b) ? 1 : 0);
    }

    @Override
    public int compare(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        int minWords = minLength / LONG_BYTES;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit. On
         * the other hand, it is substantially faster on 64-bit.
         */
        for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
            long lw = theUnsafe
                    .getLong(left, BYTE_ARRAY_BASE_OFFSET + (long) i);
            long rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET
                    + (long) i);
            long diff = lw ^ rw;

            if (diff != 0) {
                if (!littleEndian) {
                    return compareFlipped(lw, rw);
                }

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;
                if (x == 0) {
                    x = (int) (diff >>> 32);
                    n = 32;
                }

                y = x << 16;
                if (y == 0) {
                    n += 16;
                } else {
                    x = y;
                }

                y = x << 8;
                if (y == 0) {
                    n += 8;
                }
                return (int) (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * LONG_BYTES; i < minLength; i++) {
            int result = compare(left[i], right[i]);
            if (result != 0) {
                return result;
            }
        }
        return left.length - right.length;
    }

    /**
     * Compares the two specified {@code byte} values, treating them as unsigned
     * values between 0 and 255 inclusive. For example, {@code (byte) -127} is
     * considered greater than {@code (byte) 127} because it is seen as having
     * the value of positive {@code 129}.
     *
     * @param a
     *            the first {@code byte} to compare
     * @param b
     *            the second {@code byte} to compare
     * @return a negative value if {@code a} is less than {@code b}; a positive
     *         value if {@code a} is greater than {@code b}; or zero if they are
     *         equal
     */
    public static int compare(byte a, byte b) {
        return toInt(a) - toInt(b);
    }

    public static int toInt(byte value) {
        return value & UNSIGNED_MASK;
    }
}