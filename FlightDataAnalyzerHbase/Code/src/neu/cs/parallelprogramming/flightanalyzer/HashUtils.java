package neu.cs.parallelprogramming.flightanalyzer;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * Created by Tadeusz Jordan.
 * From http://josephmate.wordpress.com/2012/04/16/pitfalls-of-integer-hashcode-and-long-hashcode-with-partitioning/
 */
public class HashUtils {

    /**
     * Hash function that uses MD5.
     */
    public static int hash(Integer i, int partitions)
            throws java.security.NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(toBytes(i));
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1,digest);
        return bigInt.mod(new BigInteger(1,toBytes(partitions))).abs().intValue();
    }

    private static byte[] toBytes(Integer ii) {
        int i = ii.intValue();
        byte[] result = new byte[4];

        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /*>> 0*/);

        return result;
    }
}
