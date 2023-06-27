package com.oron.wallet.financialrecon.service.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
@Qualifier("javaRddFromTsvConverterImplV2")
public class JavaRddFromTsvConverterImplV2 implements JavaRddFromTsvConverter {

    @Autowired
    private ResourcePathRetriever retriever;

    @Autowired
    private SparkSchemaRetriever sparkSchemaRetriever;

    @Override
    public Dataset<Row> getByResourcePath(String path) throws IOException {

        String resourcePath = retriever.getResourcePath(path);

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("FinancialRecon")
                .master("local[*]")
                .getOrCreate();

        StructType schema = sparkSchemaRetriever.getFromJson("schema.json");

        // Read the TSV file as DataFrame with the specified schema
        Dataset<Row> df = spark.read()
                .option("delimiter", "\t")  // Set the delimiter to "\t" for TSV files
                .option("header", true)     // If the file has a header row
                .schema(schema)
                .csv(resourcePath);

        log.info("Dataset Count: {}", df.count());

        // Stop the SparkSession when done
        //spark.stop();
        return df;
    }
}
