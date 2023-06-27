package com.oron.wallet.financialrecon.service.helper;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Service
@Slf4j
@Qualifier("structTypeBuilderV2")
public class StructTypeBuilderImplV2 implements StructTypeBuilder {

    @Autowired
    private ResourcePathRetriever retriever;

    @Override
    public StructType build(String filePath) throws IOException {
        String resourcePath = retriever.getResourcePath(filePath);
        log.info("Resource Path: " + resourcePath);

        Gson gson = new Gson();

        String content = null;
        try {
            content = readFileToString(resourcePath);
            log.info(content);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StructType schema = gson.fromJson(content, StructType.class);
        log.info(schema.toString());
        return schema;
    }

    public static String readFileToString(String filePath) throws IOException {
        byte[] encodedBytes = Files.readAllBytes(Paths.get(filePath));
        return new String(encodedBytes, StandardCharsets.UTF_8);
    }
}
