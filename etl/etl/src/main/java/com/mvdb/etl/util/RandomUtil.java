package com.mvdb.etl.util;

public class RandomUtil
{

    private static final String[] WORDS      = { "all", "the", "world", "get", "put", "set", "harry", "berry", "kerry",
            "stand", "sit", "up", "down", "above", "below", "side", "step", "song", "span", "can", "ran", "cyan",
            "red", "yellow", "hello", "world", "plan", "now", "later", "early", "late", "find", "lose", "hurry",
            "merry", "max", "min", "average", "mode", "mutiple", "fact", "fiction", "fake", "amazing", "fort", "land",
            "sea", "air", "call", "push", "pull" };

    private static final int      WORD_COUNT = WORDS.length;

    public static String getRandomString(int wordCount)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < wordCount; i++)
        {
            int pos = (int) Math.floor(Math.random() * WORD_COUNT);
            sb.append(WORDS[pos]).append(" ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static long getRandomLong(long max)
    {
        long retval = (long) Math.floor(Math.random() * max);
        return retval;
    }

    public static long getRandomLong()
    {
        long retval = (long) Math.floor(Math.random() * Long.MAX_VALUE);
        return retval;
    }

    public static int getRandomInt(int max)
    {
        int retval = (int) Math.floor(Math.random() * max);
        return retval;
    }

    public static int getRandomInt()
    {
        int retval = (int) Math.floor(Math.random() * Integer.MAX_VALUE);
        return retval;
    }

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        String str = getRandomString(10);
        System.out.println(str);
    }

}
