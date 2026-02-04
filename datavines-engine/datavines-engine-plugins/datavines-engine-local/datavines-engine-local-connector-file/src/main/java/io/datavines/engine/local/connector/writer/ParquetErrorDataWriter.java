/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.engine.local.connector.writer;

import io.datavines.connector.api.TypeConverter;
import io.datavines.connector.api.entity.QueryColumn;
import io.datavines.connector.api.entity.ResultListWithColumns;
import io.datavines.engine.local.api.writer.ErrorDataFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datavines.common.ConfigConstants.DOUBLE_AT;

public class ParquetErrorDataWriter implements ErrorDataFileWriter {

    private ParquetWriter<GenericRecord> parquetWriter;
    private Schema avroSchema;
    private TypeConverter typeConverter;
    private List<String> fieldNames;
    private String filePath;

    @Override
    public void init(Map<String, Object> config) throws Exception {
        this.typeConverter = (TypeConverter) config.get("type_converter");
    }

    @Override
    public void open(String filePath) throws Exception {
        this.filePath = filePath;
    }

    @Override
    public void writeHeader(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getColumns())) {
            return;
        }

        List<QueryColumn> columns = resultListWithColumns.getColumns();
        fieldNames = new ArrayList<>();
        
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record("ErrorData")
                .namespace("io.datavines")
                .fields();

        for (QueryColumn column : columns) {
            String fieldName = column.getName().toLowerCase();
            if (typeConverter != null) {
                fieldName = fieldName + DOUBLE_AT + typeConverter.convert(column.getType()).toString().toLowerCase();
            }
            // Replace invalid characters for Avro field names
            String safeFieldName = fieldName.replaceAll("[^a-zA-Z0-9_]", "_");
            fieldNames.add(safeFieldName);
            fieldAssembler = fieldAssembler.optionalString(safeFieldName);
        }

        avroSchema = fieldAssembler.endRecord();

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        parquetWriter = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .build();
    }

    @Override
    public void writeRows(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getResultList())) {
            return;
        }

        if (avroSchema == null || parquetWriter == null) {
            writeHeader(resultListWithColumns);
        }

        List<QueryColumn> columns = resultListWithColumns.getColumns();
        
        for (Map<String, Object> row : resultListWithColumns.getResultList()) {
            GenericRecord record = new GenericData.Record(avroSchema);
            
            for (int i = 0; i < columns.size() && i < fieldNames.size(); i++) {
                String originalKey = columns.get(i).getName().toLowerCase();
                String fieldName = fieldNames.get(i);
                Object value = row.get(originalKey);
                record.put(fieldName, value == null ? null : String.valueOf(value).toLowerCase());
            }
            
            parquetWriter.write(record);
        }
    }

    @Override
    public void flush() throws Exception {
        // Parquet writer doesn't have explicit flush, data is written on close
    }

    @Override
    public String getFileExtension() {
        return "parquet";
    }

    @Override
    public void close() throws Exception {
        if (parquetWriter != null) {
            try {
                parquetWriter.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
