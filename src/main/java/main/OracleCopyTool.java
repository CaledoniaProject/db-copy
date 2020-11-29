package main;

import util.*;
import java.util.*;
import java.sql.*;

public class OracleCopyTool extends CopyTool
{
    public String GetInsertQuery(TableItem tableItem, int count)
    {
        List<String> marks = new ArrayList<String>();
        for (int i = 1; i <= count; i ++)
        {
            marks.add(":" + i);
        }
        return "INSERT INTO " + this.config.to_schema + "." + tableItem.name + " VALUES (" + String.join(",", marks) + ")";
    }

    public PreparedStatement GetSelectStatement(Connection conn, TableItem tableItem,long offset, long count) throws Exception
    {
        String query = "SELECT * FROM " + this.config.from_schema + "." + tableItem.name + 
                       " ORDER BY " + tableItem.getSortColumn() + " OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setLong(1, offset);
        stmt.setLong(2, count);
        return stmt;
    }

    public OracleCopyTool(Config config) throws Exception
    {
        super(config);

        this.query_getTables             = "SELECT table_name, num_rows FROM all_tables WHERE OWNER = :1";
        this.query_getAllColumns         = "SELECT table_name, column_name FROM sys.all_tab_columns WHERE owner = :1";
        this.query_getTableAndPrimaryKey = 
            "SELECT a.table_name, a.column_name " + 
            "FROM all_cons_columns a, all_constraints b " + 
            "WHERE b.constraint_type = 'P' AND b.constraint_name = a.constraint_name AND b.owner = a.owner AND b.owner = :1";
    }
}
