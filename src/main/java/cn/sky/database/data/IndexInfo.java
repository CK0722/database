package cn.sky.database.data;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * data structure describing index information
 *
 * @author Sky
 * @date 2018/4/21 上午11:24
 */
@Data
@ToString
public class IndexInfo implements Serializable {
    private static final long serialVersionUID = -3284879889186446978L;

    /**
     * the specified value
     */
    private String value;

    /**
     * a set records the index information
     */
    private List<IndexItem> items;

    public IndexInfo() {
        items = new ArrayList<>(16);
    }

    public IndexInfo(String value, int page, int row) {
        items = new ArrayList<>(16);
        this.value = value;
        this.addItem(page, row);
    }

    public void addItem(int page, int row) {
        IndexItem item = new IndexItem(page, row);
        this.items.add(item);
    }

    @Data
    public static class IndexItem implements Serializable {
        private static final long serialVersionUID = -2410945364046324694L;

        /**
         * the page number of a specified value in the dataset
         */
        private int pageNum;

        /**
         * the row number of a specified value in a specified page in the dataset
         */
        private int row;

        public IndexItem(int pageNum, int row) {
            this.pageNum = pageNum;
            this.row = row;
        }
    }
}
