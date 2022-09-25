package com.shrey.job.hdfstohive;

import com.shrey.common.service.DemoService;
import com.shrey.job.hdfstohive.etl.HdfsReader;
import com.shrey.job.hdfstohive.etl.HiveWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HdfsToHiveJob {

    private static final Logger logger = LoggerFactory.getLogger(HdfsToHiveJob.class);

    @Autowired
    private DemoService demoService;

    @Autowired
    private HdfsReader hdfsReader;

    @Autowired
    private HiveWriter hiveWriter;

    public void execute() {
        try {
            demoService.print();

            // Read
            Dataset<Row> dataset = hdfsReader.read();

            // Write
            hiveWriter.write(dataset);

        } catch (Exception e) {
            throw new RuntimeException("Unable to execute HDFS to Hive job", e);
        }
    }
}
