package com.mvdb.etl;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public abstract class TimedExecutor
{

    protected String      name             = "NamelessOperation";
    ByteArrayOutputStream baos;
    PrintStream           ps;
    long                  timeConsumedinMS = -1;

    public TimedExecutor()
    {
        baos = new ByteArrayOutputStream();
        ps = new PrintStream(baos);
    }

    public abstract void execute();

    public PrintStream getPrintStream()
    {
        return ps;
    }

    public String getOperationName()
    {
        return name;
    }

    public final long timedExecute()
    {

        long t1 = System.currentTimeMillis();
        long t2 = t1;
        try
        {
            execute();
        } catch (Throwable t)
        {
            t.printStackTrace();
        } finally
        {
            t2 = System.currentTimeMillis();
            timeConsumedinMS = t2 - t1;
            getPrintStream().println("Operation " + getOperationName() + " executed in time(ms):" + timeConsumedinMS);
            getPrintStream().flush();
            return timeConsumedinMS;
        }

    }

    public String getMessage()
    {
        return baos.toString();
    }

}
