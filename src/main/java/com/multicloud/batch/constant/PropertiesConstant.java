package com.multicloud.batch.constant;

import lombok.Getter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Configuration
@PropertySource("classpath:application.yml")
@Getter
@ToString
public class PropertiesConstant {

    public static String PROJECT_NAME;
    public static String PROJECT_VERSION;

    @Value("${spring.application.name}")
    public final void setProjectName(String projectName) {
        PROJECT_NAME = projectName;
    }

    @Value("${spring.application.version}")
    public final void setProjectVersion(String projectVersion) {
        PROJECT_VERSION = projectVersion;
    }

}