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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.connector.api.TypeConverter;
import io.datavines.connector.api.entity.QueryColumn;
import io.datavines.connector.api.entity.ResultListWithColumns;
import io.datavines.engine.local.api.writer.ErrorDataFileWriter;
import org.apache.commons.collections4.CollectionUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static io.datavines.common.ConfigConstants.DOUBLE_AT;

public class JsonErrorDataWriter implements ErrorDataFileWriter {

    private BufferedWriter writer;
    private ObjectMapper objectMapper;
    private TypeConverter typeConverter;
    private boolean firstRecord = true;
    private List<String> headerList;

    @Override
    public void init(Map<String, Object> config) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.typeConverter = (TypeConverter) config.get("type_converter");
    }

    @Override
    public void open(String filePath) throws Exception {
        writer = new BufferedWriter(new FileWriter(filePath, false));
        writer.write("[");
        writer.newLine();
        firstRecord = true;
    }

    @Override
    public void writeHeader(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getColumns())) {
            return;
        }

        List<QueryColumn> columns = resultListWithColumns.getColumns();
        headerList = new ArrayList<>();
        
        for (QueryColumn column : columns) {
            String headerKey = column.getName().toLowerCase();
            if (typeConverter != null) {
                headerKey = headerKey + DOUBLE_AT + typeConverter.convert(column.getType()).toString().toLowerCase();
            }
            headerList.add(headerKey);
        }
    }

    @Override
    public void writeRows(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getResultList())) {
            return;
        }

        if (headerList == null && CollectionUtils.isNotEmpty(resultListWithColumns.getColumns())) {
            headerList = new ArrayList<>();
            for (QueryColumn column : resultListWithColumns.getColumns()) {
                String headerKey = column.getName().toLowerCase();
                if (typeConverter != null) {
                    headerKey = headerKey + DOUBLE_AT + typeConverter.convert(column.getType()).toString().toLowerCase();
                }
                headerList.add(headerKey);
            }
        }

        List<QueryColumn> columns = resultListWithColumns.getColumns();
        
        for (Map<String, Object> row : resultListWithColumns.getResultList()) {
            Map<String, Object> jsonRow = new LinkedHashMap<>();
            
            if (columns != null && headerList != null) {
                for (int i = 0; i < columns.size() && i < headerList.size(); i++) {
                    String originalKey = columns.get(i).getName().toLowerCase();
                    String headerKey = headerList.get(i);
                    Object value = row.get(originalKey);
                    jsonRow.put(headerKey, value == null ? null : String.valueOf(value).toLowerCase());
                }
            }
            
            if (!firstRecord) {
                writer.write(",");
                writer.newLine();
            }
            writer.write("  " + objectMapper.writeValueAsString(jsonRow));
            firstRecord = false;
        }
    }

    @Override
    public void flush() throws Exception {
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public String getFileExtension() {
        return "json";
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            try {
                writer.newLine();
                writer.write("]");
                writer.flush();
                writer.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
