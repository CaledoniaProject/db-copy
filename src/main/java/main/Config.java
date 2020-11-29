package main;

import java.io.*;
import java.util.*;

public class Config 
{
    public String driver;
    public String from_url, from_username, from_password, from_schema;
    public String to_url, to_username, to_password, to_schema;
    public long max_duplicates, perpage, count;
    public boolean reverse, exclude_empty;
    public List<String> includes = new ArrayList<String>();
    public List<String> excludes = new ArrayList<String>();

    public Config (String filename) throws Exception
    {
        FileReader reader = new FileReader(filename);
        Properties props  = new Properties();
        props.load(reader);

        this.driver          = props.getProperty("driver");
        this.reverse         = Boolean.parseBoolean(props.getProperty("reverse", "false"));
        this.exclude_empty   = Boolean.parseBoolean(props.getProperty("exclude_empty", "true"));
        this.max_duplicates  = Long.parseLong(props.getProperty("max_duplicates", "10000"), 10);
        this.perpage         = Long.parseLong(props.getProperty("perpage", "1000"), 10);
        this.count           = Long.parseLong(props.getProperty("count", "500000"), 10);
        this.from_url        = props.getProperty("from_url");
        this.from_username   = props.getProperty("from_username");
        this.from_password   = props.getProperty("from_password");
        this.from_schema     = props.getProperty("from_schema");
        this.to_url          = props.getProperty("to_url");
        this.to_username     = props.getProperty("to_username");
        this.to_password     = props.getProperty("to_password");
        this.to_schema       = props.getProperty("to_schema");

        if (this.driver == null || this.max_duplicates <= 0 || this.perpage <= 0 || this.count <= 0)
        {
            throw new Exception("Invalid basic configuration");
        }            

        if (this.from_url == null || this.from_username == null || this.from_password == null)
        {
            throw new Exception("Invalid source configuration");
        }

        if (this.to_url == null || this.to_username == null || this.to_password == null)
        {
            throw new Exception("Invalid target configuration");
        }

        // Oracle 必须指定 schema
        if (this.isOracle() && (this.from_schema == null || this.to_schema == null))
        {
            throw new Exception("Invalid configuration for Oracle database");
        }

        Class.forName(this.driver);

        String includes = props.getProperty("includes", "").trim();
        if (includes.length() > 0)
            this.includes = Arrays.asList(includes.split(","));

        String excludes = props.getProperty("excludes", "").trim();
        if (excludes.length() > 0)
            this.excludes = Arrays.asList(excludes.split(","));
    }

    public boolean isOracle()
    {
        return this.driver.equals("oracle.jdbc.driver.OracleDriver");
    }

    public boolean isMySQL()
    {
        return this.driver.equals("com.mysql.jdbc.Driver") || this.driver.equals("com.mysql.cj.jdbc.Driver"); 
    }

    public boolean isPG()
    {
        return this.driver.equals("org.postgresql.Driver");
    } 
    
    public boolean isSQLServer()
    {
        return this.driver.equals("com.microsoft.jdbc.sqlserver.SQLServerDriver");
    }
}
