package cn.sky.database.util;

import cn.sky.database.constant.BusinessEnum;
import cn.sky.database.constant.Constants;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * @author Sky
 * @date 2018/4/19 上午10:27
 */
public class DbUtil {

    private static final String DB_SUFFIX = ".csv";

    /**
     * split the dataset into different pages
     *
     * @param datasetPath
     * @param dbPath
     * @param fileSize
     * @return
     * @throws IOException
     */
    public static int splitDataSet(String datasetPath, String dbPath, int fileSize) throws IOException {
        CSVReader csvReader = CsvUtil.readFile(datasetPath);
        Iterator<String[]> iterator = csvReader.iterator();

        List<String[]> contents = new ArrayList<>(fileSize);
        String[] header = iterator.next();
        String[] next = null;
        int fileTag = 1;
        contents.add(header);
        while (iterator.hasNext()) {
            next = iterator.next();
            contents.add(next);

            if (contents.size() >= fileSize) {
                CsvUtil.writeFile(dbPath + Constants.REPOSITORY_FILE_PREFIX + fileTag, contents);
                contents.clear();
                ++fileTag;
                contents.add(header);
            }
        }

        if (contents.size() > 1) {
            CsvUtil.writeFile(dbPath + Constants.REPOSITORY_FILE_PREFIX + fileTag, contents);
        }
        return fileTag;
    }

    public static List<String> queryDataWithoutIndex(String column, String targetValue, String dbPath, int threadSize) {
        BusinessEnum businessEnum = BusinessEnum.valueOf(column);
        if (null == businessEnum) {
            System.err.println("sorry: the specified column dose not exits.");
            System.err.println("the exits value are: " + BusinessEnum.values().toString());
        }
        ExecutorService executorService = ThreadUtil.getExecutorService(threadSize);
        CountDownLatch downLatch = new CountDownLatch(threadSize);

        long start = System.currentTimeMillis();
        for (int i = 1; i <= threadSize; ++i) {
            executorService.submit(new QueryThreader(i, threadSize, downLatch, dbPath, targetValue, businessEnum));
        }

        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int fileCount = CsvUtil.getFileCount(dbPath);
        int m = fileCount / threadSize;
        downLatch = new CountDownLatch(1);
        executorService.submit(new QueryThreader(m * threadSize + 1, 1, downLatch, dbPath, targetValue, businessEnum));
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        System.out.println("query " + businessEnum.getField() + "=" + targetValue + " without index cost:" + (end - start) / 1000.0 + " seconds.");
        return Lists.newArrayList();
    }


    private static class QueryThreader implements Callable<List<String[]>> {

        private int initFileTag;
        private int step;
        private CountDownLatch downLatch;
        private String dbPath;
        private String targetValue;
        private BusinessEnum column;

        public QueryThreader(int initFileTag, int step, CountDownLatch downLatch, String dbPath, String targetValue, BusinessEnum column) {
            this.initFileTag = initFileTag;
            this.step = step;
            this.downLatch = downLatch;
            this.dbPath = dbPath;
            this.targetValue = targetValue;
            this.column = column;
        }

        @Override
        public List<String[]> call() throws Exception {
            if (Strings.isNullOrEmpty(targetValue)) {
                System.err.println("the target value cann't be null!");
            }

            final int columnIndex = column.getIndex();
            List<String[]> res = new ArrayList<>(256);
            while (true) {
                String fileName = dbPath + Constants.REPOSITORY_FILE_PREFIX + initFileTag + DB_SUFFIX;
                List<String[]> contents = CsvUtil.readContents(fileName);
                if (null == contents || contents.isEmpty()) {
                    break;
                }

                contents.stream().filter((cols) -> {
                    if (null == cols || cols.length <= columnIndex) {
                        return false;
                    }
                    if (targetValue.equalsIgnoreCase(cols[columnIndex])) {
                        return true;
                    }
                    return false;
                }).forEach(cols -> {
                    res.add(cols);
                    System.out.println("find target: " + StringUtils.join(cols, " "));
                });
                initFileTag += step;
            }

            downLatch.countDown();
            return res;
        }
    }
}
