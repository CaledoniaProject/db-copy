package main;

import util.*;
import java.sql.*;
import java.util.*;

import com.mysql.cj.xdevapi.SqlStatement;
import com.mysql.cj.xdevapi.Table;

public class PGCopyTool extends CopyTool
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
                       " ORDER BY " + tableItem.getSortColumn() + " LIMIT ? OFFSET ?";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setLong(1, count);
        stmt.setLong(2, offset);
        return stmt;
    }

    public String GetSelectQuery(TableItem tableItem)
    {
        return "SELECT * FROM " + this.config.from_schema + "." + tableItem.name +
               " ORDER BY " + tableItem.getSortColumn() + " LIMIT ? OFFSET ?";
    }

    public PGCopyTool(Config config) throws Exception
    {
        super(config);

        this.query_getTables             = "SELECT relname as table_name, reltuples as rows FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname = ? AND relkind = 'r' ORDER BY reltuples DESC";
        this.query_getAllColumns         = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = ? ORDER BY ordinal_position ASC";
        this.query_getTableAndPrimaryKey = 
            "SELECT tab.table_name, string_agg(kcu.column_name, ', ') AS columns " +
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
