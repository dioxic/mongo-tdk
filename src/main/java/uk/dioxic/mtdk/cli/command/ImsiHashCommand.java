package uk.dioxic.mtdk.cli.command;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ImsiHashCommand {
    static MessageDigest md;

    static long[] imsiList = new long[]{520031234567890L, 310150123456789L, 502130123456789L, 460001357924680L, 470010171566423L, 313460000000001L};

    public static void main(String[] args) {
        for (long imsi : imsiList) {

            long modHash = modHash(imsi, 720);
            long encoded = encode(imsi);

            System.out.println(String.format("imsi: %s", imsi));
            System.out.println(String.format("hash: %s", longHash(imsi)));
            System.out.println(String.format("modHash: %s", modHash));
            System.out.println(String.format("encoded: %s", encoded));
            System.out.println();
            System.out.println(String.format("imsi:                   %s", Long.toBinaryString(imsi)));
            System.out.println(String.format("modHash: %s", String.format("%1$10s", Long.toBinaryString(modHash)).replace(' ', '0')));
            System.out.println(String.format("encoded: %s", String.format("%1$64s", Long.toBinaryString(encoded)).replace(' ', '0')));
            System.out.println();

            if (decodeSuffix(encoded) != imsi) {
                throw new RuntimeException("expected: " + imsi + " found: " + decodeSuffix(encoded));
            }

            if (decodePrefix(encoded) != modHash) {
                throw new RuntimeException("expected: " + modHash + " found: " + decodePrefix(encoded));
            }
        }
    }

    static {
        try {
            md = MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return result;
    }

    static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static long longHash(long input) {
        return bytesToLong(md.digest(longToBytes(input)));
    }

    public static long modHash(long input, int modulus) {
        return Math.abs(input % modulus);
    }

    public static long encode(long l) {
        return encode(modHash(l, 720), l);
    }

    /**
     * Combine the prefix and suffix using the 10 MSB for the prefix and the 54 LSB for the suffix
     */
    public static long encode(long prefix, long suffix) {
        return ((prefix & 0x3FF) << 54) | suffix;
    }

    /**
     * Take the 54 LSBs
     */
    public static long decodeSuffix(long l) {
        return l & 0x3FFFFFFFFFFFFFL;
    }

    /**
     * Take the 10 MSBs
     */
    public static int decodePrefix(long l) {
        return (int)(l >> 54) & 0x3FF;
    }
}