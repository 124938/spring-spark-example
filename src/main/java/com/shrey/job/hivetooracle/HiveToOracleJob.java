package com.shrey.job.hivetooracle;

import com.shrey.job.hivetooracle.etl.HiveReader;
import com.shrey.job.hivetooracle.etl.OracleWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HiveToOracleJob {

    private static final Logger logger = LoggerFactory.getLogger(HiveToOracleJob.class);

    @Autowired
    private HiveReader hiveReader;

    @Autowired
    private OracleWriter oracleWriter;

    public void execute() {
        try {
            // Read
            Dataset<Row> dataset = hiveReader.read();

            // Write
            oracleWriter.write(dataset);
        } catch (Exception e) {
            throw new RuntimeException("Unable to execute HIVE to Oracle job", e);
        }
    }
}
