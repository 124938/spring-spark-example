package com.shrey.job.hivetooracle;

import com.shrey.common.config.ApplicationConfig;
import com.shrey.job.hdfstohive.HdfsToHiveJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class HiveToOracleBootstrap {

    private static final Logger logger = LoggerFactory.getLogger(HiveToOracleBootstrap.class);

    public static void main(String[] args) {
        // Initiate spring application context
        logger.info("=========== Building spring application context - start ===========");
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(ApplicationConfig.class);
        logger.info("=========== Building spring application context - end ===========");

        // start job
        logger.info("=========== HIVE to Oracle job - start ===========");
        applicationContext
                .getBean(HiveToOracleJob.class)
                .execute();
        logger.info("=========== HIVE to Oracle job - end ===========");
    }
}
