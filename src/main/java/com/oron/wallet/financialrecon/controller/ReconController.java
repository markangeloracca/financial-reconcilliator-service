package com.oron.wallet.financialrecon.controller;

import com.google.gson.Gson;
import com.oron.wallet.financialrecon.service.FinancialReconService;
import com.oron.wallet.financialrecon.service.helper.DataFrameFactory;
import com.oron.wallet.financialrecon.service.helper.JavaRddFromTsvConverter;
import com.oron.wallet.financialrecon.service.helper.ResourcePathRetriever;
import com.oron.wallet.financialrecon.service.helper.SparkSchemaRetriever;
import com.oron.wallet.financialrecon.service.helper.JavaRddFromTsvConverterImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class ReconController {



    //@Autowired
    //private SparkSchemaRetriever retriever;

    //@Autowired
    //private ResourcePathRetriever resourcePathRetriever;

    //@Autowired
    //private DataFrameFactory dataFrameFactory;

    @Autowired
    private FinancialReconService reconService;

    @PostMapping("/recon")
    public ResponseEntity<String> processRecon() throws Exception {

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("FinancialRecon")
                .master("local[*]")
                .getOrCreate();

        String successResponse = "Recon process was completed successfully.";

        reconService.recon("tsv/Sample.tsv", "tsv/Sample_New.tsv");

        //JavaRDD<String> rdd2 = converter.getByResourcePath(fileSampleB);


      //  converter.getByResourcePath(fileSampleA);

   //    rdd1.foreach(element -> System.out.println("RDD1: " + element));
        //StructType schema = retriever.getFromJson("schema.json");


        //Dataset<Row> dataset1 = dataFrameFactory.create(generateJavaRDDRow(rdd1));
        //Dataset<Row> dataset2 = dataFrameFactory.create(generateJavaRDDRow(rdd2));



        //String[] columns = dataset1.columns();
        //JavaRDD<Row> column1Values = dataset1.select("name").toJavaRDD();

//        for (String column : columns) {
//            JavaRDD<Row> column1Values = dataset1.select(column).toJavaRDD();
//            JavaRDD<Row> column2Values = dataset2.select(column).toJavaRDD();
//            List<Row> rowList = column1Values.collect();
//            String test = rowList.get(0).getString(0);
//            log.info("TEST STRING: " + test);
//
//
//
//            // Perform the comparison on the two RDDs, for example, using the `zip()` function.
//            JavaRDD<Boolean> comparisonResult = column1Values.zip(column2Values).map(tuple -> tuple._1().equals(tuple._2()));
//
//            // Collect and print the comparison results
//            List<Boolean> resultList = comparisonResult.collect();
//            System.out.println("Comparison result for column '" + column + "': " + resultList);
//        }

        spark.stop();
        return new ResponseEntity<>(successResponse, HttpStatus.OK);
    }


    public JavaRDD<Row> generateJavaRDDRow(JavaRDD<String> rdd) {
        JavaRDD<Row> rowRDD = rdd.map(json -> {
            Gson gson = new Gson();
            Object[] fields = gson.fromJson(json, Object[].class);
            return RowFactory.create(fields);
        });
        return rowRDD;
    }

}
