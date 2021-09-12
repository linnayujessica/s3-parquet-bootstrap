package com.zipToParquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.IOException;
import java.util.List;

public class CsvParquetWriter extends ParquetWriter<List<String>> {

    public CsvParquetWriter(org.apache.hadoop.fs.Path file, WriteSupport<List<String>> writeSupport) throws IOException {
        super(file, writeSupport);
    }
}
