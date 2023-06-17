package com.oron.wallet.financialrecon;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRunner {

    public static void main(String[] args) {
        // Create a JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("TsvFileReader");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the TSV file as a JavaRDD
        String filePath = "/path/to/your/file.tsv";
        JavaRDD<String> lines = sc.textFile(filePath);

        // Perform further operations on the RDD
        // For example, you can split each line by tabs to get individual fields
        JavaRDD<String[]> data = lines.map(line -> line.split("\t"));

        // Print the contents of the RDD
        data.foreach(line -> {
            for (String field : line) {
                System.out.print(field + "\t");
            }
            System.out.println();
        });

        // Stop the SparkContext
        sc.stop();
    }
}
