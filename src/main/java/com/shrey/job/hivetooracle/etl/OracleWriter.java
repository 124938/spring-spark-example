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
public class OracleWriter {

    private static final Logger logger = LoggerFactory.getLogger(OracleWriter.class);

    @Autowired
    private SparkSession sparkSession;

    public void write(final Dataset<Row> dataset) {
        try {

            logger.info("##### Write data to hive : start #####");

            // Write data to oracle table
            dataset.write()
                    .mode(SaveMode.Append)
                    .format("jdbc")
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .option("url", "jdbc:oracle:thin:@localhost:1521/xe")
                    .option("dbtable", "TBL_ECO_SURVEY")
                    .option("user", "system")
                    .option("password", "Test@123")
                    .save();
            logger.info("#####= Write data to Oracle : end #####");

            logger.info("#####= Verification of Oracle data : start #####");

            Dataset<Row> tmpDS = sparkSession.read()
                    .format("jdbc")
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .option("url", "jdbc:oracle:thin:@localhost:1521/xe")
                    .option("dbtable", "TBL_ECO_SURVEY")
                    .option("user", "system")
                    .option("password", "Test@123")
                    .option("fetchsize", 1000)
                    .load();

            tmpDS.registerTempTable("tmp");

            // Verify data from hive table
            this.sparkSession
                    .sql("select count(1) from tmp")
                    .show(100, false);

            Dataset<Row> ecoSurManDS = this.sparkSession
                    .sql("select * from tmp");

            ecoSurManDS.printSchema();
            ecoSurManDS.show(20, false);

            logger.info("#####= Verification of hive data : end #####");
        } catch (Exception e) {
            throw new RuntimeException("Unable to write data to oracle", e);
        }
    }
}
