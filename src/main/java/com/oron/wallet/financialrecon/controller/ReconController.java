package com.oron.wallet.financialrecon.controller;

import com.google.gson.Gson;
import com.oron.wallet.financialrecon.service.helper.ResourcePathRetriever;
import com.oron.wallet.financialrecon.service.helper.SparkSchemaRetriever;
import com.oron.wallet.financialrecon.service.helper.TsvToJavaRddConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class ReconController {

    @Autowired
    private TsvToJavaRddConverter converter;

    @Autowired
    private SparkSchemaRetriever retriever;

    @Autowired
    private ResourcePathRetriever resourcePathRetriever;

    @PostMapping("/recon")
    public ResponseEntity<String> processRecon() throws Exception {
        String successResponse = "Recon process was completed successfully.";

        String fileSampleA = "tsv/Sample.tsv";
        JavaRDD<String> rdd1 = converter.getByResourcePath(fileSampleA);

        String fileSampleB = "tsv/Sample_New.tsv";
        JavaRDD<String> rdd2 = converter.getByResourcePath(fileSampleB);

        StructType schema = retriever.getFromJson("schema.json");

        String jsonSchemaPath = "schema.json";
        String jsonSchemaResource = resourcePathRetriever.getResourcePath(jsonSchemaPath);
        Gson gson = new Gson();

//        Dataset<Row> df1 = rdd1.toDF();
//        Dataset<Row> df2 = rdd2.toDF();

        return new ResponseEntity<>(successResponse, HttpStatus.OK);
    }

}
