package cn.sky.database.controller;

import cn.sky.database.util.DbUtil;
import com.alibaba.fastjson.JSONArray;

/**
 * @author Sky
 * @date 2018/4/24 下午9:40
 */
public class QueryWithoutIndexController {
    private static final String methodName = "Query Data without Index Method";

    public static void main(String[] args) {
        usage();
        if (null == args || args.length != 4) {
            System.err.println("sorry: the " + methodName + " need 4 arguments.");
            return;
        }

        String dbPath = args[0];
        String column = args[1];
        String targetValue = args[2];
        int threadSize = Integer.valueOf(args[3]);
        System.out.println("args = " + JSONArray.toJSONString(args));

        try {
            long start = System.currentTimeMillis();
            System.out.println(methodName + " has started...");
            DbUtil.queryDataWithoutIndex(column, targetValue, dbPath, threadSize);
            long end = System.currentTimeMillis();
            System.out.println("query " + column + "=" + targetValue + " without index successfully and it costs:" + (end - start) / 1000.0 + " seconds.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void usage() {
        System.out.println();
        System.out.println("-----------" + methodName + " usage------------");
        System.out.println("args[0]: the absolute path of the dataset which has been split.");
        System.out.println("args[1]: the column name of the dataset.(such as bn_name,bn_abn,etc.)");
        System.out.println("args[2]: the value to be searched(such as 96010711145).");
        System.out.println("args[3]: the maximum number of threads to search data(such as 10).");
    }
}
