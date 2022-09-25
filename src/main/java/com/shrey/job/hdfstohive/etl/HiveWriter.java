package com.shrey.job.hdfstohive.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HiveWriter {

    private static final Logger logger = LoggerFactory.getLogger(HiveWriter.class);

    @Autowired
    private SparkSession sparkSession;

    public void write(final Dataset<Row> dataset) {
        try {

            logger.info("##### Write data to hive : start #####");

            // Create hive database if not exists
            this.sparkSession
                    .sql("show databases")
                    .show(100, false);

            this.sparkSession
                    .sql("create database if not exists test_db")
                    .show(100, false);

            this.sparkSession
                    .sql("show tables in test_db")
                    .show(100, false);

            // Write data to hive table
            dataset.write()
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .saveAsTable("test_db.economic_survey_of_manufacturing_june_2022");

            logger.info("#####= Write data to hive : end #####");

            logger.info("#####= Verification of hive data : start #####");
            this.sparkSession
                    .sql("show tables in test_db")
                    .show(100, false);

            // Verify data from hive table
            this.sparkSession
                    .sql("select count(1) from test_db.economic_survey_of_manufacturing_june_2022")
                    .show(100, false);

            Dataset<Row> ecoSurManDS = this.sparkSession
                    .sql("select * from test_db.economic_survey_of_manufacturing_june_2022");

            ecoSurManDS.printSchema();
            ecoSurManDS.show(20, false);

            logger.info("#####= Verification of hive data : end #####");
        } catch (Exception e) {
            throw new RuntimeException("Unable to write data to hive", e);
        }
    }
}
