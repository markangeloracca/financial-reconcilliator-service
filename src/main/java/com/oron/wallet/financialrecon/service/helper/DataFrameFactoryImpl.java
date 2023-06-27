package com.oron.wallet.financialrecon.service.helper;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class DataFrameFactoryImpl implements DataFrameFactory {




    @Autowired
    private SparkSchemaRetriever retriever;



    @Override
    public Dataset<Row> create(JavaRDD<Row> rowRDD) throws IOException {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("FinancialRecon")
                .master("local[*]")
                .getOrCreate();

        StructType schema = retriever.getFromJson("schema.json");
        Dataset<Row> dataset = spark.createDataFrame(rowRDD, schema);
        //spark.textFile

        //spark.close();
       // spark.stop();
        return dataset;
    }
}
