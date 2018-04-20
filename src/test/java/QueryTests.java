import cn.sky.database.Application;
import cn.sky.database.config.DbConfig;
import cn.sky.database.constant.BusinessEnum;
import cn.sky.database.util.DbUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

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

    @Test
    public void testQueryBnameWithoutIndex() {
        DbUtil.queryDataWithoutIndex(BusinessEnum.BN_NAME.getField(), targetBname, dbConfig.getRepositoryPath(), dbConfig.getThreadPoolSize());
    }
}
