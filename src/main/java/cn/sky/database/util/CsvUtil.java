package cn.sky.database.util;

import cn.sky.database.constant.BusinessEnum;
import com.google.common.base.Strings;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

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

    public static void print(CSVReader csvReader) {
        Iterator<String[]> iterator = csvReader.iterator();
        int row = 1;
        while (iterator.hasNext()) {
            String[] datas = iterator.next();

            System.out.print("row=" + row + "-->");
            for (int i = 1; i < datas.length; i++) {
                if (!Strings.isNullOrEmpty(datas[i])) {
                    System.out.print(BusinessEnum.getFieldByIndex(i) + ":" + datas[i]+"; ");
                }
            }
            System.out.println();

            ++row;
        }
    }

}
