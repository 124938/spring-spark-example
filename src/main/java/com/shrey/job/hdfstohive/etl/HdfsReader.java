package com.shrey.job.hdfstohive.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HdfsReader {

    private static final Logger logger = LoggerFactory.getLogger(HdfsReader.class);

    @Autowired
    private SparkSession sparkSession;

    public Dataset<Row> read() {
        try {
            logger.info("=========== Read data from CSV file : start ==========");
            // Create schema
            StructType structure = new StructType(
                    new StructField[]{
                            new StructField("Series_reference", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("Period", DataTypes.FloatType, false, Metadata.empty()),
                            new StructField("Data_value", DataTypes.DoubleType, false, Metadata.empty()),
                            new StructField("Suppressed", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("STATUS", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("UNITS", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("Magnitude", DataTypes.IntegerType, false, Metadata.empty()),
                            new StructField("Subject", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("Group", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("Series_title_1", DataTypes.StringType, false, Metadata.empty())
                    }
            );

            // Create dataset from CSV file
            Dataset<Row> datasetEcoManSurvey = sparkSession
                    .read()
                    .option("header", "true")
                    .schema(structure)
                    .csv("src/main/resources/economic-survey-of-manufacturing-june-2022.csv");

            // Print schema
            datasetEcoManSurvey.printSchema();

            // Display sample records
            datasetEcoManSurvey.show(10, false);
            logger.info("=========== Read data from CSV file : end ==========");

            return datasetEcoManSurvey;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read data from CSV file", e);
        }
    }
}
