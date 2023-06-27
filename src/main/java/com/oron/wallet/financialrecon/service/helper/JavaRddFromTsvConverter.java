package com.oron.wallet.financialrecon.service.helper;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public interface JavaRddFromTsvConverter {
    Dataset<Row> getByResourcePath(String path) throws IOException;
}
