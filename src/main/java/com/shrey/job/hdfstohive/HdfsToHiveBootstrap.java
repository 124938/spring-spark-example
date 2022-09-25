package com.shrey.job.hdfstohive;

import com.shrey.common.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class HdfsToHiveBootstrap {

    private static final Logger logger = LoggerFactory.getLogger(HdfsToHiveBootstrap.class);

    public static void main(String[] args) {
        // Initiate spring application context
        logger.info("=========== Building spring application context - start ===========");
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(ApplicationConfig.class);
        logger.info("=========== Building spring application context - end ===========");

        // start job
        logger.info("=========== HDFS to HIVE job - start ===========");
        applicationContext
                .getBean(HdfsToHiveJob.class)
                .execute();
        logger.info("=========== HDFS to HIVE job - end ===========");
    }
}
