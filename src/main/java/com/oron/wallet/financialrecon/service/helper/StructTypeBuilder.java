package com.oron.wallet.financialrecon.service.helper;

import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public interface StructTypeBuilder {
    StructType build(String filePath) throws IOException;
}
