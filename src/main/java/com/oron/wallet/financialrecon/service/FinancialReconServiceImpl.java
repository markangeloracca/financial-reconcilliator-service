package com.oron.wallet.financialrecon.service;

import com.oron.wallet.financialrecon.service.helper.JavaRddFromTsvConverter;
import com.oron.wallet.financialrecon.service.helper.SparkSchemaRetriever;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class FinancialReconServiceImpl implements FinancialReconService {

    @Autowired
    @Qualifier("javaRddFromTsvConverterImplV2")
    private JavaRddFromTsvConverter converter;

    @Autowired
    private SparkSchemaRetriever retriever;

    @Override
    public void recon(String path1, String path2) throws IOException {
        String fileSampleA = path1;
        Dataset<Row> df1 = converter.getByResourcePath(fileSampleA);

        String fileSampleB = path2;
        Dataset<Row> df2 = converter.getByResourcePath(fileSampleB);
        String[] columns1 = df1.columns();
        //String[] columns2 = df2.columns();

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("FinancialRecon")
                .master("local[*]")
                .getOrCreate();

        StructType schema = retriever.getFromJson("schema.json");
        // Join the two datasets based on a common column
       // Dataset<Row> joinedDataset = df1.join(df2, df1.col("name").equalTo(df2.col("name")));
       // System.out.println("Dataset Count Joined: " + joinedDataset.count());
        //String[] columnsJoined = joinedDataset.columns();
        //log.info("JOINED COLUMNS: {}", Arrays.toString(columnsJoined));
//        joinedDataset.foreach(row -> {
//            String rowString = row.toString();
//            log.info("ROWSTRING: {}", rowString);
//        });

       // JavaRDD<Row> rdd1 = df1.toJavaRDD();
        List<Row> rowList = df1.toJavaRDD().collect();

        // Create a JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String ID_COLUMN = "name";
        int diffRowCtr = 0;
        int diffRowLowCtr = 0;
        log.info("Hello World!");
        List<String> idsToBeUpdated = new ArrayList<>();
        for(Row row : rowList) {
            log.info("BASE ROW per row - {}", row.toString());
            JavaRDD<Row> rrdItem1 = sc.parallelize(Arrays.asList(row));
            JavaRDD<Row> rrdItem2 = df2.select("*").where(df2.col(ID_COLUMN)
                    .equalTo(row.get(0))).javaRDD();
            if(rrdItem2.count() < 1) {
                // DO NOTHING
                continue; }
            rrdItem2.foreach(row1 -> {
                log.info("RRDITEM2_ITEMS_PREVIEW - {}", row1.toString());
            });

            /* Create Datasets */
            Dataset<Row> datasetA  = spark.createDataFrame(rrdItem1, schema);
            Dataset<Row> datasetB  = spark.createDataFrame(rrdItem2, schema);

            String rowId = row.getString(0);
            log.info("ROW_ID: {}", rowId);
            // Create an empty list to store the columns with differences
            List<String> differentColumns = new ArrayList<>();

            for (String column : columns1) {
                Row column1Value = datasetA.select(column).first();
                Row column2Value = datasetB.select(column).first();

                String value1 = column1Value.getString(0);
                String value2 = column2Value.getString(0);

                log.info("column1Value - {} \t\t| column2Value - {}", value1, value2);

                boolean hasDifferences = !value1.equals(value2);
                // If differences are found, add the column name to the list
                if (hasDifferences) {
                    differentColumns.add("rowId: " + rowId + " column: " + column);

                    log.info("RowID: {} | Column: {} | OldValue: {} | NewValue: {}",
                            rowId, column, value1, value2);
                    diffRowLowCtr++;
                }
            }
            if(differentColumns.size() > 0) {
                idsToBeUpdated.add(rowId);
                diffRowCtr++;
            }
        }

        log.info("IDs to be updated {}.", idsToBeUpdated);
        log.info("Total {} rows with differences.", diffRowCtr);
        log.info("Total {} columns with differences.", diffRowLowCtr);
        log.info("Hello World Ending!");
    }
}
