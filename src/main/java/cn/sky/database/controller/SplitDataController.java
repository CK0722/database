package cn.sky.database.controller;

import cn.sky.database.util.DbUtil;
import com.alibaba.fastjson.JSONArray;

import java.io.IOException;

/**
 * @author Sky
 * @date 2018/4/22 上午11:45
 */
public class SplitDataController {

    private static final String methodName = "SplitData Method";

    public static void main(String[] args) {
        usage();
        if (null == args || args.length != 3) {
            System.err.println("sorry: the " + methodName + " need 3 arguments.");
            return;
        }

        String datasetFile = args[0];
        String dbPath = args[1];
        int rowSize = Integer.valueOf(args[2]);
        System.out.println("args = " + JSONArray.toJSONString(args));

        try {
            long start = System.currentTimeMillis();
            System.out.println(methodName + " has started...");
            int pages = DbUtil.splitDataSet(datasetFile, dbPath, rowSize);
            long end = System.currentTimeMillis();
            System.out.println("split dataset into " + pages + " files successfully and it costs: " + (end - start) / 1000.0 + " seconds.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void usage() {
        System.out.println();
        System.out.println("-----------" + methodName + " usage------------");
        System.out.println("args[0]: the absolute path of the dataset.");
        System.out.println("args[1]: the absolute path of the repository.");
        System.out.println("args[2]: the maximum row of the records in each file of the repository.");
    }
}
