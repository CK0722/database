package cn.sky.database.config;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author Sky
 * @date 2018/4/18 下午1:07
 */
@Data
@ToString
@Component
@ConfigurationProperties(prefix = "datasource")
public class DbConfig {

    private String originalFile;

    private int fileSize;

}