package com.mvdb.scratch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

public class HadoopClientTest
{

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        
        //String dataFileName = "/tmp/df.txt";
        String sequenceFileName = "/tmp/seq.dat";
        String hadoopLocalFS = "file:///";
        /*
        createDataFile(dataFileName);
        testWriteSequenceFile(dataFileName, sequenceFileName, hadoopLocalFS);
        testReadSequenceFile(sequenceFileName, hadoopLocalFS);
        */
        testWriteSequenceFile(new int[][] {{1,2}, {3,4}}, sequenceFileName, hadoopLocalFS);
    }

    
    public static void testReadSequenceFile(String sequenceFileName, String hadoopLocalFS)
    {
        try
        {
            HadoopClient.readSequenceFile(sequenceFileName, hadoopLocalFS) ;
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    private static void createDataFile(String dataFileName)
    {
        int numOfLines = 20;
        String baseStr = "....Test...";
        List<String> lines = new ArrayList<String>();
        for (int i = 0; i < numOfLines; i++)
            lines.add(i + baseStr + UUID.randomUUID());

        File dataFile = new File(dataFileName);
        try
        {
            FileUtils.writeLines(dataFile, lines, true);
            Thread.sleep(2000);
        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static void testWriteSequenceFile(String dataFileName, String sequenceFileName, String hadoopLocalFS)
    {
        try
        {
            File dataFile = new File(dataFileName);
            HadoopClient.writeToSequenceFile(dataFile, sequenceFileName, hadoopLocalFS);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    
    public static void testWriteSequenceFile(int[][] values, String sequenceFileName, String hadoopLocalFS)
    {
        try
        {
            HadoopClient.writeToSequenceFile(values, sequenceFileName, hadoopLocalFS);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

}
