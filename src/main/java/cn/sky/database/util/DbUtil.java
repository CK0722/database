package cn.sky.database.util;

import cn.sky.database.constant.BusinessEnum;
import cn.sky.database.data.IndexInfo;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Sky
 * @date 2018/4/19 上午10:27
 */
public class DbUtil {

    public static final String REPOSITORY_FILE_PREFIX = "DB";
    private static final String DB_DATA_SUFFIX = ".csv";
    private static final String DB_INDEX_SUFFIX = ".index";

    /**
     * split the dataset into different files and then save these files in the repository
     *
     * @param datasetPath the absolute path of the dataset
     * @param dbPath      the absolute path of the repository
     * @param fileSize    the maximun number of recordes in each file
     * @return the maximum row of the records in each file of the repository
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
                CsvUtil.writeFile(buildDbFileName(dbPath, fileTag), contents);
                contents.clear();
                ++fileTag;
                contents.add(header);
            }
        }

        if (contents.size() > 1) {
            CsvUtil.writeFile(buildDbFileName(dbPath, fileTag), contents);
        }
        return fileTag;
    }


    /**
     * search data in dataset without index
     *
     * @param column      the name of the column has been indexed
     * @param targetValue the value to be searched
     * @param dbPath      the absolute path of the dataset which has been split
     * @param threadSize  the maximum number of threads to search data
     * @return a set of recordes related to the targetValue in the dataset
     */
    public static List<String> queryDataWithoutIndex(String column, String targetValue, String dbPath, int threadSize) {
        BusinessEnum businessEnum = checkColumn(column);
        if (null == businessEnum) {
            return Lists.newArrayList();
        }

        ExecutorService executorService = ThreadUtil.getExecutorService(threadSize);
        CountDownLatch downLatch = new CountDownLatch(threadSize);

        long start = System.currentTimeMillis();
        List<Future<List<String[]>>> futures = new ArrayList<>(threadSize + 1);
        Future<List<String[]>> future = null;
        for (int i = 1; i <= threadSize; ++i) {
            future = executorService.submit(new QueryThreaderNoIndex(i, threadSize, downLatch, dbPath, targetValue, businessEnum));
            futures.add(future);
        }

        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int fileCount = CsvUtil.getFileCount(dbPath);
        int m = fileCount / threadSize;
        downLatch = new CountDownLatch(1);
        future = executorService.submit(new QueryThreaderNoIndex(m * threadSize + 1, 1, downLatch, dbPath, targetValue, businessEnum));
        futures.add(future);
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        System.out.println("query " + businessEnum.getField() + "=" + targetValue + " without index cost:" + (end - start) / 1000.0 + " seconds.");

        ArrayList<String> res = Lists.newArrayList();
        futures.forEach(f -> {
            try {
                List<String[]> target = f.get();
                if (null != target || !target.isEmpty()) {
                    res.add(StringUtils.join(target, " "));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return res;
    }


    /**
     * search data in the dataset with index
     *
     * @param column      the name of the column has been indexed
     * @param targetValue the value to be searched
     * @param indexPath   the absolute path of the index file
     * @param dbPath      the absolute path of the dataset which has been split
     * @param capcity
     * @return a set of recordes related to the targetValue in the dataset
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static List<String> queryDataWithIndex(String column, String targetValue, String indexPath, String dbPath, int capcity) throws IOException, ClassNotFoundException {
        if (Strings.isNullOrEmpty(targetValue)) {
            System.err.println("sorry: the targetValue cann't be " + targetValue);
            return Lists.newArrayList();
        }

        BusinessEnum businessEnum = checkColumn(column);
        if (null == businessEnum) {
            return Lists.newArrayList();
        }

        String fileName = getIndexFileName(column, indexPath);
        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("sorry: the index file dose not exits. You must create index file:" + fileName + " firstly");
            return Lists.newArrayList();
        }
        List<List<IndexInfo>> index = getIndexTable(file);

        long start = System.currentTimeMillis();
        List<String> targets = new ArrayList<>(16);
        int hashIndex = getIndex(targetValue, capcity);
        List<IndexInfo> indexInfos = index.get(hashIndex);
        for (IndexInfo indexInfo : indexInfos) {
            if (targetValue.equalsIgnoreCase(indexInfo.getValue())) {

                ArrayListMultimap<Integer, Integer> pageRows = ArrayListMultimap.create();
                List<IndexInfo.IndexItem> items = indexInfo.getItems();
                items.forEach(i -> {
                    pageRows.put(i.getPageNum(), i.getRow());
                });

                Set<Integer> pages = pageRows.keySet();
                pages.forEach(p -> {
                    String dbFileName = buildDbFileName(dbPath, p);
                    try {
                        List<String> target = CsvUtil.readContentsByLineNumer(dbFileName, pageRows.get(p));
                        if (null != target && !target.isEmpty()) {
                            targets.addAll(target);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                });
            }
        }

        long end = System.currentTimeMillis();
        System.out.println();
        targets.forEach(t -> {
            System.out.println("find target: " + t);
        });
        System.out.println("find the target:" + targetValue + " cost " + (end - start) + " ms.");
        return targets;
    }


    /**
     * create an index based on the specified column in the dataset
     *
     * @param dbPath    the absolute path of the dataset which has been split
     * @param column    the name of the column in the dataset to be indexed
     * @param indexPath the absolute path of the index file
     * @param capcity   the maximum capcity of the index table
     * @return
     * @throws IOException
     */
    public static boolean createIndex(String dbPath, String column, String indexPath, int capcity) throws IOException {
        BusinessEnum businessEnum = checkColumn(column);
        if (null == businessEnum) {
            return false;
        }

        File file = new File(indexPath);
        if (!file.isDirectory()) {
            System.err.println("sorry: the specified index path does not exists.");
            return false;
        }

        if (capcity <= 0) {
            System.err.println("sorry: the value of the capcity must be positive.");
            return false;
        }

        List<List<IndexInfo>> indexs = new ArrayList<>(capcity);
        for (int i = 0; i < capcity; ++i) {
            indexs.add(new ArrayList<>(16));
        }

        int columnIndex = businessEnum.getIndex();
        int fileCount = CsvUtil.getFileCount(dbPath);
        for (int i = 1; i <= fileCount; ++i) {
            String fileName = dbPath + REPOSITORY_FILE_PREFIX + i + DB_DATA_SUFFIX;
            List<String[]> contents = CsvUtil.readContents(fileName);
            if (null == contents || contents.isEmpty()) {
                continue;
            }

            for (int j = 0; j < contents.size(); j++) {
                String[] cols = contents.get(j);
                if (null == cols || cols.length <= columnIndex) {
                    continue;
                }
                if (Strings.isNullOrEmpty(cols[columnIndex])) {
                    continue;
                }
                String colValue = cols[columnIndex];
                int hashIndex = getIndex(colValue, capcity);

                boolean hasIndex = false;
                List<IndexInfo> indexInfos = indexs.get(hashIndex);
                for (IndexInfo info : indexInfos) {
                    if (colValue.equalsIgnoreCase(info.getValue())) {
                        info.addItem(i, j + 1);
                        hasIndex = true;
                        break;
                    }
                }
                if (!hasIndex) {
                    IndexInfo indexInfo = new IndexInfo(colValue, i, j + 1);
                    indexInfos.add(indexInfo);
                }
            }
        }

        FileOutputStream fout = new FileOutputStream(getIndexFileName(column, indexPath));
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(indexs);
        oos.flush();
        oos.close();

        return true;
    }


    /**
     * create the database file name
     *
     * @param dbPath  the absolute path of the dataset which has been split
     * @param fileTag database file number
     * @return the database file name based on the fileTag
     */
    private static String buildDbFileName(String dbPath, int fileTag) {
        return dbPath + REPOSITORY_FILE_PREFIX + fileTag + DB_DATA_SUFFIX;
    }


    /**
     * load the index table of the dataset
     *
     * @param file the index file
     * @return a table that records index information
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static List<List<IndexInfo>> getIndexTable(File file) throws IOException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        FileInputStream fin = new FileInputStream(file);
        ObjectInputStream oin = new ObjectInputStream(fin);
        Object o = oin.readObject();
        List<List<IndexInfo>> index = (List<List<IndexInfo>>) o;
        if (null == index || index.isEmpty()) {
            System.err.println("sorry: the index file:" + file.getName() + " is empty.");
            throw new RuntimeException("sorry: the index file:" + file.getName() + " is empty.");
        }
        System.out.println("load the index file cost:" + (System.currentTimeMillis() - start) / 1000 + " seconds.");
        return index;
    }


    /**
     * calculate the index position based on the specified value
     *
     * @param value   a value to be indexed
     * @param capcity the maximum capcity of index table
     * @return
     */
    private static int getIndex(String value, int capcity) {
        int index = Math.abs(value.toLowerCase().hashCode()) % capcity;
        return index;
    }


    /**
     * get the index file name
     *
     * @param column    the column to be indexed
     * @param indexPath the absolute path of the index file
     * @return the index file name
     */
    private static String getIndexFileName(String column, String indexPath) {
        return indexPath + column + DB_INDEX_SUFFIX;
    }


    /**
     * check whether the column exits in the dataset
     *
     * @param column the name of the column in the dataset
     * @return a valid column enum or NULL
     */
    private static BusinessEnum checkColumn(String column) {
        BusinessEnum businessEnum = BusinessEnum.valueOf(column.toUpperCase());
        if (null == businessEnum) {
            System.err.println("sorry: the specified column dose not exits.");
            System.err.println("the exits value are: " + BusinessEnum.values().toString());
        }
        return businessEnum;
    }


    /**
     * the static inner thread class to search data without index information
     */
    private static class QueryThreaderNoIndex implements Callable<List<String[]>> {

        private int initFileTag;
        private int step;
        private CountDownLatch downLatch;
        private String dbPath;
        private String targetValue;
        private BusinessEnum column;

        public QueryThreaderNoIndex(int initFileTag, int step, CountDownLatch downLatch, String dbPath, String targetValue, BusinessEnum column) {
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

            try {
                final int columnIndex = column.getIndex();
                List<String[]> res = new ArrayList<>(256);
                int fileCount = CsvUtil.getFileCount(dbPath);
                while (initFileTag <= fileCount) {
                    String fileName = dbPath + REPOSITORY_FILE_PREFIX + initFileTag + DB_DATA_SUFFIX;
                    List<String[]> contents = CsvUtil.readContents(fileName);
                    if (null == contents || contents.isEmpty()) {
                        continue;
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
                return res;
            } catch (Exception e) {
                new RuntimeException(e);
            } finally {
                downLatch.countDown();
            }

            return Lists.newArrayList();
        }
    }
}
