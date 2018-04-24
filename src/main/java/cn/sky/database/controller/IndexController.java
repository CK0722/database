package cn.sky.database.controller;

import cn.sky.database.util.DbUtil;
import com.alibaba.fastjson.JSONArray;

import java.io.IOException;

/**
 * @author SongBaoYu
 * @date 2018/4/24 下午9:09
 */
public class IndexController {

    private static final String methodName = "Create Index Method";

    public static void main(String[] args) {
        usage();
        if (null == args || args.length != 4) {
            System.err.println("sorry: the " + methodName + " need 4 arguments.");
            return;
        }

        String dbPath = args[0];
        String indexPath = args[1];
        String column = args[2];
        int capcity = Integer.valueOf(args[3]);
        System.out.println("args = " + JSONArray.toJSONString(args));

        try {
            long start = System.currentTimeMillis();
            System.out.println(methodName + " has started...");
            DbUtil.createIndex(dbPath, column, indexPath, capcity);
            long end = System.currentTimeMillis();
            System.out.println("create the index of " + column + " successfully and it costs:" + (end - start) / 1000 + " seconds.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void usage() {
        System.out.println();
        System.out.println("-----------" + methodName + " usage------------");
        System.out.println("args[0]: the absolute path of the repository.");
        System.out.println("args[1]: the absolute path of the index file.");
        System.out.println("args[2]: the column name of the dataset(such as bn_name,bn_abn,etc.)");
        System.out.println("args[3]: the maximum capcity of the index table.");
    }
}
