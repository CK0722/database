import cn.sky.database.Application;
import cn.sky.database.config.DbConfig;
import cn.sky.database.constant.BusinessEnum;
import cn.sky.database.util.CsvUtil;
import cn.sky.database.util.DbUtil;
import com.opencsv.CSVReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @author SongBaoYu
 * @date 2018/4/18 下午1:26
 */

@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class UtilTests {

    @Autowired
    private DbConfig dbConfig;

    @Test
    public void testReadCsvFile() {
        String originalFile = dbConfig.getDatasetFile();
        try {
            CSVReader csvReader = CsvUtil.readFile(originalFile);
            CsvUtil.print(csvReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSplitDataSet() {
        try {
            DbUtil.splitDataSet(dbConfig.getDatasetFile(), dbConfig.getRepositoryPath(), dbConfig.getFileSize());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateBnameIndex() {
        try {
            DbUtil.createIndex(dbConfig.getRepositoryPath(), BusinessEnum.BN_NAME.getField(), dbConfig.getIndexPath(), dbConfig.getIndexTableSize());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateAbnIndex() {
        try {
            DbUtil.createIndex(dbConfig.getRepositoryPath(), BusinessEnum.BN_ABN.getField(), dbConfig.getIndexPath(), dbConfig.getIndexTableSize());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
