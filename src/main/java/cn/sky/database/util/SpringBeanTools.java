package cn.sky.database.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;

/**
 * @author Sky
 */
@Configuration
public class SpringBeanTools implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext context) {
        applicationContext = context;
    }

    public static Object getBean(Class<?> classname) {
        try {
            Object _restTemplate = applicationContext.getBean(classname);
            return _restTemplate;
        } catch (Exception e) {
            return "";
        }
    }

    public static void setApplicationContext1(ApplicationContext context) {
        applicationContext = context;
    }
}