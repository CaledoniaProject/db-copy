package main;

import util.*;
import java.sql.*;
import java.util.*;

public abstract class CopyTool
{
    public Config config;
    public HashMap<String, TableItem> tables;

    public String query_getTables, query_getTableAndPrimaryKey, query_getAllColumns;

    public abstract String GetInsertQuery(TableItem tableItem, int count);
    public abstract PreparedStatement GetSelectStatement(Connection conn, TableItem tableItem, long offset, long count) throws Exception;
    
    public CopyTool(Config config) throws Exception
    {
        this.config = config;
    }

    public static CopyTool getInstance(String filename) throws Exception
    {
        Config config = new Config(filename);
        if (config.isOracle())
        {
            return new OracleCopyTool(config);
        }

        if (config.isMySQL())
        {
            return new MySQLCopyTool(config);
        }

        if (config.isPG())
        {
            return new PGCopyTool(config);
        }

        return null;
    }

    public Connection GetConnection(ConnectionType connectionType) throws Exception
    {
        if (connectionType == ConnectionType.FROM)
        {
            return DriverManager.getConnection(
                this.config.from_url,
                this.config.from_username,
                this.config.from_password);
        }

        return DriverManager.getConnection(
            this.config.to_url,
            this.config.to_username,
            this.config.to_password);
    }

    public ArrayList<String> GetMissingTables() throws Exception
    {
        HashMap<String, TableItem> localTables = this.GetAllTable(ConnectionType.TO);
        ArrayList<String> result = new ArrayList<String>();

        for (String name : this.tables.keySet())
        {
            if (! localTables.containsKey(name))
            {
                result.add(name);
            }
        }

        return result;
    }

    /**
     * 获取所有表信息
     */
    public HashMap<String, TableItem> GetAllTable(ConnectionType connectionType) throws Exception
    {
        HashMap<String, TableItem> result = new HashMap<String, TableItem>();
        Connection connection = null;

        try
        {
            connection = this.GetConnection(connectionType);

            // 获取所有表
            PreparedStatement stmt = connection.prepareStatement(this.query_getTables);
            stmt.setObject(1, this.config.from_schema);

            ResultSet rs = stmt.executeQuery();
            while (rs.next())
            {
                TableItem tableItem = new TableItem();
                tableItem.name  = rs.getString(1);
                tableItem.count = rs.getInt(2);

                result.put(rs.getString(1), tableItem);
            }

            // 获取主键
            PreparedStatement stmt2 = connection.prepareStatement(this.query_getTableAndPrimaryKey);
            stmt2.setObject(1, this.config.from_schema);

            ResultSet rs2 = stmt2.executeQuery();
            while (rs2.next())
            {
                TableItem tableItem = result.get(rs2.getString(1));

                // MySQL columns 可能返回 NULL
                if (rs2.getString(2) != null)
                {
                    tableItem.primaryKey = Arrays.asList(rs2.getString(2).trim().split("\\s*,\\s*"));
                }
            }

            // 获取所有列信息
            PreparedStatement stmt3 = connection.prepareStatement(this.query_getAllColumns);
            stmt3.setObject(1, this.config.from_schema);

            ResultSet rs3 = stmt3.executeQuery();
            while (rs3.next())
            {
                TableItem tableItem = result.get(rs3.getString(1));
                tableItem.columns.add(rs3.getString(2));
            }
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }

        return result;
    }

    public void CopyTable(TableItem tableItem) throws Exception
    {
        boolean has_error = false, is_restarted = false;
        long offset = 0, total = 0, duplicates = 0, success = 0;

        while (! has_error && total < this.config.count)
        {
            Connection conn1 = null, conn2 = null;

            try
            {
                conn1 = this.GetConnection(ConnectionType.FROM);
                conn2 = this.GetConnection(ConnectionType.TO);

                if (offset % 10000 == 0 || is_restarted)
                {
                    System.out.println("offset=" + offset + "; duplicates=" + duplicates);
                    is_restarted = false;
                }

                // 连续 10K 重复内容，直接退出
                if (duplicates >= this.config.max_duplicates)
                {
                    System.out.println("Too many duplicates, finishing table early");
                    break;
                }
                
                PreparedStatement stmt1 = this.GetSelectStatement(conn1, tableItem, offset, this.config.perpage);

                ResultSet rs = stmt1.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();

                if (! rs.isBeforeFirst())
                {
                    System.out.println("No more rows in " + tableItem.name + ", total read " + offset);
                    break;
                }

                String query2 = this.GetInsertQuery(tableItem, rsmd.getColumnCount());
                while (rs.next())
                {
                    // 如果没有主键，只能手动 SELECT 测试是否存在
                    if (tableItem.primaryKey == null)
                    {

                    }

                    // 其他情况直接看数据库是否报错
                    PreparedStatement stmt2 = conn2.prepareStatement(query2);
                    for (int i = 1; i <= rsmd.getColumnCount(); i ++)
                    {
                        stmt2.setObject(i, rs.getObject(i));
                    }
                    
                    // 忽略重复的内容
                    try
                    {
                        stmt2.executeUpdate();
                        success ++;
                    }
                    catch (org.postgresql.util.PSQLException e)
                    {
                        // unique_violation
                        if (e.getSQLState().equals("23505"))
                        {
                            duplicates ++;
                        }
                        else
                        {
                            System.out.println("SQL STATE: " + e.getSQLState());
                            throw e;
                        }
                    }
                    catch (SQLException e)
                    {
                        if (this.config.isOracle() && e.getErrorCode() == 1)
                        {
                            duplicates ++;
                        }
                        else if (this.config.isMySQL() && e.getErrorCode() == 1062)
                        {
                            duplicates ++;
                        }
                        else
                        {
                            System.out.println("ERROR CODE: " + e.getErrorCode());
                            throw e;
                        }
                    }
                    finally
                    {
                        if (stmt2 != null)
                        {
                            stmt2.close();
                        }
                    }
                    
                    offset ++;
                    total ++;
                }
                
                rs.close();
                stmt1.close();
            }
            catch (SQLRecoverableException e)
            {
                e.printStackTrace();
                break;
            }
            catch (SQLException e)
            {
                e.printStackTrace();
                break;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                break;
            }
            finally
            {
                if (conn1 != null)
                {
                    conn1.close();
                }

                if (conn2 != null)
                {
                    conn2.close();
                }
            }
        }

        System.out.println("Read " + total + " rows in total, succeed " + success + ", duplicates " + duplicates);
    }

    public void Start() throws Exception
    {
        // 获取复制目标列表
        ArrayList<TableItem> allTables = this.GetCopyList();
        if (allTables.size() == 0)
        {
            System.out.println("No tables to copy");
            return;
        }

        System.out.println("\nCopy options");
        System.out.println("Reverse order:      " + this.config.reverse);
        System.out.println("Rows to copy:       " + this.config.count);
        System.out.println("Maximum duplicates: " + this.config.max_duplicates);
        System.out.println("Data perpage:       " + this.config.perpage);

        long no = 1;
        for (TableItem tableItem : allTables)
        {
            System.out.printf("\n[ %d / %d ] Copying %s sorted by %s (%s rows%s)\n",
                no ++, allTables.size(), tableItem.name, tableItem.getSortColumn(), tableItem.count, 
                tableItem.primaryKey == null ? ", no primary key" : "");
            this.CopyTable(tableItem);
        }
    }

    public ArrayList<TableItem> GetCopyList() throws Exception
    {
        // 获取全部表信息
        this.tables = this.GetAllTable(ConnectionType.FROM);

        // 首先按照表大小排序
        ArrayList<TableItem> allTables = new ArrayList<TableItem>(this.tables.values());
        Collections.sort(allTables, new TableItemComparator());

        if (this.config.reverse)
        {
            Collections.reverse(allTables);
        }

        // 白名单过滤
        if (this.config.includes.size() > 0)
        {
            HashSet<String> wanted = new HashSet<String>(this.config.includes);
            for (TableItem tableItem : allTables)
            {
                if (! wanted.contains(tableItem.name))
                {
                    tableItem.excludeReason = ExcludeReason.USER_EXCLUDE;
                }
            }
        }

        // 黑名单
        for (String name : this.config.excludes)
        {
            TableItem tableItem = this.tables.get(name);
            if (tableItem != null)
            {
                tableItem.excludeReason = ExcludeReason.USER_EXCLUDE;
            }
        }
        
        ArrayList<String> missingTables = this.GetMissingTables();        
        for (TableItem tableItem : allTables)
        {
            // 本地不存在的表
            if (missingTables.contains(tableItem.name))
            {
                tableItem.excludeReason = ExcludeReason.MISSING;
            }

            // 不可排序的表
            else if (tableItem.getSortColumn() == null)
            {
                tableItem.excludeReason = ExcludeReason.NOT_SORTABLE;
            }

            // 空表
            else if (tableItem.count == 0 && this.config.exclude_empty)
            {
                tableItem.excludeReason = ExcludeReason.EMPTY;
            }
        }

        // 打印表名称和行数
        System.out.println("\nDumping row counts");
        for (TableItem tableItem : allTables)
        {
            if (tableItem.excludeReason == ExcludeReason.NONE)
                System.out.printf("%-50s %d\n", tableItem.name, tableItem.count);
        }
        
        // 打印排除原因
        System.out.println("");
        ArrayList<TableItem> result = new ArrayList<TableItem>();
        for (TableItem tableItem : allTables)
        {
            switch (tableItem.excludeReason)
            {
                case MISSING:
                    System.out.println("Warning: " + tableItem.name + " is missing locally (row count: " + tableItem.count + ")");
                    break;
                case NOT_SORTABLE:
                    System.out.println("Warning: " + tableItem.name + " has no sortable column (row count: " + tableItem.count + ")");
                    break;
                case EMPTY:
                    break;
                case USER_EXCLUDE:
                    break;
                case NONE:
                    result.add(tableItem);
                    break;
            }
        }

        return result;
    }

    /**
     * 确保连接可用
     */
    public void EnsureValid() throws Exception
    {
        try
        {
            Connection connection = this.GetConnection(ConnectionType.FROM);
            connection.close();
        }
        catch (Exception e)
        {
            throw new Exception("Can't connect to remote server: " + e);
        }

        try
        {
            Connection connection = this.GetConnection(ConnectionType.TO);
            connection.close();
        }
        catch (Exception e)
        {
            throw new Exception("Can't connect to local server: " + e);
        }
    }
}
