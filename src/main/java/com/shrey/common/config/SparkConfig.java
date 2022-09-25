package com.shrey.common.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String name;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.warehouse}")
    private String warehouseDir;

    private SparkConf buildSparkConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName(this.name);
        conf.setMaster(this.master);
        if (this.master.startsWith("local")) {
            conf.set("spark.sql.warehouse.dir", warehouseDir);
        }

        return conf;
    }

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .config(buildSparkConf())
                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }
}
