package cn.sky.database.controller;

import cn.sky.database.util.DbUtil;
import com.alibaba.fastjson.JSONArray;

/**
 * @author SongBaoYu
 * @date 2018/4/24 下午9:54
 */
public class QueryDataWithIndexController {
    private static final String methodName = "Query Data with Index Method";

    public static void main(String[] args) {
        usage();
        if (null == args || args.length != 5) {
            System.err.println("sorry: the " + methodName + " need 5 arguments.");
            return;
        }

        String dbPath = args[0];
        String indexPath = args[1];
        String column = args[2];
        String targetValue = args[3];
        int capcity = Integer.valueOf(args[4]);
        System.out.println("args = " + JSONArray.toJSONString(args));

        try {
            long start = System.currentTimeMillis();
            System.out.println(methodName + " has started...");
            DbUtil.queryDataWithIndex(column, targetValue, indexPath, dbPath, capcity);
            long end = System.currentTimeMillis();
            System.out.println(methodName + " finished successfully and it costs:" + (end - start) / 1000.0 + " seconds(load index file included).");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void usage() {
        System.out.println();
        System.out.println("-----------" + methodName + " usage------------");
        System.out.println("args[0]: the absolute path of the dataset which has been split.");
        System.out.println("args[1]: the absolute path of the index file.");
        System.out.println("args[2]: the column name of the dataset.(such as bn_name,bn_abn,etc.)");
        System.out.println("args[3]: the value to be searched(such as 96010711145).");
        System.out.println("args[4]: the maximum capcity of the index table(such as 10000).");
    }
}
