package cn.sky.database.util;

import cn.sky.database.constant.BusinessEnum;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sky
 * @date 2018/4/18 下午1:11
 */
public class CsvUtil {


    /**
     * @param fileName
     * @return a set of the main contents of the specified file that will ignore the header
     * @throws IOException
     */
    public static List<String[]> readContents(String fileName) throws IOException {
        CSVReader reader = readFile(fileName);
        if (null == reader) {
            return Lists.newArrayList();
        }

        reader.readNext();
        List<String[]> contents = reader.readAll();
        return contents;
    }


    /**
     * save the contents into the specified file
     *
     * @param fileName
     * @param contents
     * @return
     * @throws IOException
     */
    public static boolean writeFile(String fileName, List<String[]> contents) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.getParentFile().mkdir();
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file);
        CSVWriter csvWriter = new CSVWriter(fileWriter, '\t');
        csvWriter.writeAll(contents);
        return true;
    }


    /**
     * read records of fixed line numbers and will ignore the file header
     *
     * @param fileName
     * @param rows     a set of line numbers
     * @return a set of records of fixed line numbers
     * @throws IOException
     */
    public static List<String> readContentsByLineNumer(String fileName, List<Integer> rows) throws IOException {
        CSVReader csvReader = readFile(fileName);
        csvReader.readNext();           //ignore the file header

        int maxRow = 1;
        for (Integer row : rows) {
            if (row > maxRow) {
                maxRow = row;
            }
        }

        List<String> targets = new ArrayList<>(rows.size());
        for (int i = 1; i <= maxRow; ++i) {
            String[] data = csvReader.readNext();
            if (null == data || 0 == data.length) {
                break;
            }

            if (rows.contains(i)) {
                targets.add(StringUtils.join(data, " "));
            }
        }
        return targets;
    }


    /**
     * the number of files in the absolute directory
     *
     * @param path a absolute directory
     * @return
     */
    public static int getFileCount(String path) {
        File file = new File(path);
        if (!file.exists() || !file.isDirectory()) {
            System.err.println("sorry: the specified file path:" + path + " does not exits.");
            return 0;
        }
        File[] files = file.listFiles();
        return files.length;
    }


    /**
     * print the data in the CSVReader
     *
     * @param csvReader
     */
    public static void print(CSVReader csvReader) {
        Iterator<String[]> iterator = csvReader.iterator();
        int row = 1;
        iterator.next();
        while (iterator.hasNext()) {
            String[] datas = iterator.next();

            System.out.print("row=" + row + "-->");
            for (int i = 1; i < datas.length; i++) {
                if (!Strings.isNullOrEmpty(datas[i])) {
                    System.out.print(BusinessEnum.getFieldByIndex(i) + ":" + datas[i] + "; ");
                }
            }
            System.out.println();

            ++row;
        }
    }

    /**
     * @param fileName the name of the file
     * @return a reader of csv file type
     * @throws IOException
     */
    public static CSVReader readFile(String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            throw new RuntimeException("sorry: the file:" + fileName + " dose not exits!");
        }

        FileReader fileReader = new FileReader(file);
        CSVReader csvReader = new CSVReader(fileReader, '\t');
        return csvReader;
    }


}
