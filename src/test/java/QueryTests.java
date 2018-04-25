import cn.sky.database.Application;
import cn.sky.database.config.DbConfig;
import cn.sky.database.constant.BusinessEnum;
import cn.sky.database.util.DbUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @author Sky
 * @date 2018/4/20 下午11:21
 */
@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class QueryTests {

    @Autowired
    private DbConfig dbConfig;
    private static final String targetBname = "Warby Wares";
    private static final String targetAbn = "96010711145";

    @Test
    public void testQueryBnameWithoutIndex() {
        DbUtil.queryDataWithoutIndex(BusinessEnum.BN_NAME.getField(), targetBname, dbConfig.getRepositoryPath(), dbConfig.getThreadPoolSize());
    }

    @Test
    public void testQueryAbnWithoutIndex() {
        DbUtil.queryDataWithoutIndex(BusinessEnum.BN_ABN.getField(), targetAbn, dbConfig.getRepositoryPath(), dbConfig.getThreadPoolSize());
    }

    @Test
    public void testQueryBnameWithIndex() {
        try {
            DbUtil.queryDataWithIndex(BusinessEnum.BN_NAME.getField(), targetBname, dbConfig.getIndexPath(), dbConfig.getRepositoryPath(), dbConfig.getIndexTableSize());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryAbnWithIndex() {
        try {
            DbUtil.queryDataWithIndex(BusinessEnum.BN_ABN.getField(), targetAbn, dbConfig.getIndexPath(), dbConfig.getRepositoryPath(), dbConfig.getIndexTableSize());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
