package com.shrey.job.hivetooracle.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HiveReader {

    private static final Logger logger = LoggerFactory.getLogger(HiveReader.class);

    @Autowired
    private SparkSession sparkSession;

    public Dataset<Row>  read() {
        try {

            logger.info("##### Reading data from hive : start #####");

            // Verify data from hive table
            this.sparkSession
                    .sql("show databases")
                    .show(100, false);

            this.sparkSession
                    .sql("show tables in test_db")
                    .show(100, false);

            this.sparkSession
                    .sql("desc test_db.economic_survey_of_manufacturing_june_2022")
                    .show(100, false);

            this.sparkSession
                    .sql("select count(1) from test_db.economic_survey_of_manufacturing_june_2022")
                    .show(100, false);

            Dataset<Row> ecoSurManDS = this.sparkSession
                    .sql("" +
                            " select " +
                            "   Series_reference," +
                            "   Period," +
                            "   Data_value," +
                            "   Suppressed," +
                            "   STATUS," +
                            "   UNITS," +
                            "   Magnitude," +
                            "   Series_title_1" +
                            " from " +
                            "   test_db.economic_survey_of_manufacturing_june_2022");

            ecoSurManDS.printSchema();
            ecoSurManDS.show(20, false);

            logger.info("##### Reading data from hive : end #####");
            return ecoSurManDS;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read data from hive", e);
        }
    }
}
