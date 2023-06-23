package com.oron.wallet.financialrecon.service.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class SparkSchemaRetriever {


    @Autowired
    @Qualifier("structTypeBuilderV1")
    private StructTypeBuilder builder;
    public StructType getFromJson(String filePath) throws IOException {

        StructType schema = builder.build(filePath); // gson.fromJson(content, StructType.class);
        log.info("schema size: " + schema.size());
        log.info(schema.toString());

        return schema;
    }



}
