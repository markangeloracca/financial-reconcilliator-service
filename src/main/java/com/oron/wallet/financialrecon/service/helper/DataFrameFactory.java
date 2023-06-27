package com.oron.wallet.financialrecon.service.helper;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public interface DataFrameFactory {

    Dataset<Row> create(JavaRDD<Row> rowRDD) throws IOException;

}
