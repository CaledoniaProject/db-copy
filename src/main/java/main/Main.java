package main;

import util.*;

public class Main 
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 1)
        {
            System.out.println("Usage: db-copy config.ini");
            return;
        }

        if (! Util.FileExists(args[0]))
        {
            System.out.println("No such file: " + args[0]);
            return;
        }

        CopyTool copyTool = CopyTool.getInstance(args[0]);
        copyTool.EnsureValid();
        copyTool.Start();
    }
}
