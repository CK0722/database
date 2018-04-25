package cn.sky.database;

import cn.sky.database.util.SpringBeanTools;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Sky
 * @date 2018/4/18 上午10:49
 */

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class);
        SpringBeanTools.setApplicationContext1(context);
    }
}
