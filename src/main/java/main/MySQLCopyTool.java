package main;

import util.*;
import java.util.*;
import java.sql.*;

public class MySQLCopyTool extends CopyTool
{
    public String GetInsertQuery(TableItem tableItem, int count)
    {
        List<String> marks = new ArrayList<String>();
        for (int i = 1; i <= count; i ++)
        {
            marks.add("?");
        }
        return "INSERT INTO " + this.config.to_schema + "." + tableItem.name + " VALUES (" + String.join(",", marks) + ")";
    }

    public PreparedStatement GetSelectStatement(Connection conn, TableItem tableItem,long offset, long count) throws Exception
    {
        String query = "SELECT * FROM " + this.config.from_schema + "." + tableItem.name +
                       " ORDER BY " + tableItem.getSortColumn() + " LIMIT ?, ?";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setLong(1, offset);
        stmt.setLong(2, count);
        return stmt;
    }

    public MySQLCopyTool(Config config) throws Exception
    {
        super(config);

        this.query_getTables             = "SELECT TABLE_NAME, SUM(TABLE_ROWS) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? GROUP BY TABLE_NAME";
        this.query_getAllColumns         = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = ? ORDER BY ordinal_position ASC";
        this.query_getTableAndPrimaryKey = 
            "SELECT tab.table_name, group_concat(kcu.column_name ORDER BY kcu.ordinal_position separator ', ') AS columns " +
            "FROM information_schema.tables tab " +
            "LEFT JOIN information_schema.table_constraints tco " +
            "ON tab.table_schema = tco.table_schema " +
            "AND tab.table_name = tco.table_name " +
            "AND tco.constraint_type = 'PRIMARY KEY' " +
            "LEFT JOIN information_schema.key_column_usage kcu " +
            "ON tco.constraint_schema = kcu.constraint_schema " +
            "AND tco.constraint_name = kcu.constraint_name " +
            "AND tco.table_name = kcu.table_name " +
            "WHERE tab.table_schema = ? AND tab.table_type = 'BASE TABLE' GROUP BY tab.table_schema, tab.table_name, tco.constraint_name";
    }
}
