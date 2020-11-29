package util;

import java.util.Comparator;

public class TableItemComparator implements Comparator<TableItem> {
    @Override
    public int compare(TableItem a, TableItem b) {
        if (a.count > b.count) {
            return -1;
        } else if (a.count == b.count) {
            return 0;
        }

        return 1;
    }
}
