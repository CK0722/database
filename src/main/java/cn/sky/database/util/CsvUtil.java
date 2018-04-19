package cn.sky.database.util;

import cn.sky.database.constant.BusinessEnum;
import com.google.common.base.Strings;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sky
 * @date 2018/4/18 下午1:11
 */
public class CsvUtil {

    public static CSVReader readFile(String fileName) throws IOException {
        FileReader fileReader = new FileReader(fileName);
        CSVReader csvReader = new CSVReader(fileReader, '\t');
        return csvReader;
    }

    public static boolean writeFile(String fileName, List<String[]> contents) throws IOException {
        File file = new File(fileName + ".csv");
        if (!file.exists()) {
            file.getParentFile().mkdir();
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file);
        CSVWriter csvWriter = new CSVWriter(fileWriter,'\t');
        csvWriter.writeAll(contents);
        return true;
    }


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

}
