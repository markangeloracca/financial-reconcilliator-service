package com.oron.wallet.financialrecon.service.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

@Service
@Slf4j
@Qualifier("structTypeBuilderV1")
public class StructTypeBuilderImplV1 implements StructTypeBuilder {

    @Autowired
    private ResourcePathRetriever retriever;

    @Override
    public StructType build(String filePath) throws IOException {
        String resourcePath = retriever.getResourcePath(filePath);
        log.info("Resource Path: " + resourcePath);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(new File(resourcePath));

        return jsonToStructType(jsonNode);
    }

    private StructType jsonToStructType(JsonNode jsonNode) {
        JsonNode fields = jsonNode.get("fields");
        if (fields.isArray()) {
            StructField[] structFields = new StructField[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                JsonNode fieldNode = fields.get(i);
                String name = fieldNode.get("name").asText();
                String type = fieldNode.get("type").asText();
                boolean nullable = fieldNode.get("nullable").asBoolean();
                //StructType fieldType = jsonToStructType(fieldNode.get("fields"));
                structFields[i] = new StructField(name, convertType(type), nullable, Metadata.empty());
            }
            return new StructType(structFields);
        } else {
            return null; // Handle if needed
        }
    }

    private DataType convertType(String type) {
        // Map the type string to the corresponding DataType
        switch (type.toLowerCase()) {
            case "string":
                return DataTypes.StringType;
            case "integer":
                return DataTypes.IntegerType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
                return DataTypes.createDecimalType();
            // Add more cases for other types as needed
            default:
                return null;
        }
    }


}
