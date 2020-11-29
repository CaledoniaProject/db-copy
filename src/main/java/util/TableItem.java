package util;

import java.util.*;

public class TableItem {
    public String        name;
    public List<String>  primaryKey;
    public long          count;
    public boolean       isExcluded;
    public ExcludeReason excludeReason;
    public List<String>  columns;

    public TableItem()
    {
        this.isExcluded    = false;
        this.excludeReason = ExcludeReason.NONE;
        this.columns       = new ArrayList<String>();
    }

    public String getSortColumn()
    {
        ArrayList<String> keys = new ArrayList<String>();
        if (this.primaryKey != null)
        {
            for (String key : this.primaryKey)
            {
                keys.add(key + " DESC");
            }
            return String.join(", ", keys);
        }

        return null;
        // return this.columns.get(0);
    }
}
