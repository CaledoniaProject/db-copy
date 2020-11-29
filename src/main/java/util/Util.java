package util;

import java.io.*;

public class Util
{    
    public static Boolean FileExists(String filename)
    {
        return new File(filename).exists();
    }
}
