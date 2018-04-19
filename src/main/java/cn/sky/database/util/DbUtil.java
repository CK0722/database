package cn.sky.database.util;

import cn.sky.database.constant.Constants;
import com.opencsv.CSVReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sky
 * @date 2018/4/19 上午10:27
 */
public class DbUtil {

    /**
     * split the dataset into different pages
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
}
