package com.oron.wallet.financialrecon.service.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TsvToJavaRddConverter {

    @Autowired
    private ResourcePathRetriever retriever;

    public JavaRDD<String> getByResourcePath(String path) {
        // Create a JavaSparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("TsvFileReader");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourcePath = retriever.getResourcePath(path);
        JavaRDD<String> lines = sc.textFile(resourcePath);

        // Perform further operations on the RDD
        // For example, you can split each line by tabs to get individual fields
        JavaRDD<String[]> data = lines.map(line -> line.split("\t"));

        // Print the contents of the RDD
        data.foreach(line -> {
            for (String field : line) {
                log.info(field + "\t");
            }
            System.out.println();
        });

        // Stop the SparkContext
        sc.stop();

        return null;
    }


}
