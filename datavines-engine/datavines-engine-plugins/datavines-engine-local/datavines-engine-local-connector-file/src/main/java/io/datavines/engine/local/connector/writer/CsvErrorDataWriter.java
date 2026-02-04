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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datavines.common.ConfigConstants.DOUBLE_AT;

public class CsvErrorDataWriter implements ErrorDataFileWriter {

    private CSVPrinter csvPrinter;
    private BufferedWriter writer;
    private String columnSeparator = ",";
    private TypeConverter typeConverter;
    private List<String> headerList;

    @Override
    public void init(Map<String, Object> config) throws Exception {
        this.columnSeparator = (String) config.getOrDefault("column_separator", ",");
        this.typeConverter = (TypeConverter) config.get("type_converter");
    }

    @Override
    public void open(String filePath) throws Exception {
        writer = new BufferedWriter(new FileWriter(filePath, true));
        char delimiter = columnSeparator.length() > 0 ? columnSeparator.charAt(0) : ',';
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .build();
        csvPrinter = new CSVPrinter(writer, csvFormat);
    }

    @Override
    public void writeHeader(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getColumns())) {
            return;
        }

        List<QueryColumn> columns = resultListWithColumns.getColumns();
        headerList = new ArrayList<>();
        List<String> headerDisplayList = new ArrayList<>();
        
        for (QueryColumn column : columns) {
            String headerKey = column.getName().toLowerCase();
            String headerDisplay = headerKey;
            if (typeConverter != null) {
                headerDisplay = headerKey + DOUBLE_AT + typeConverter.convert(column.getType()).toString().toLowerCase();
            }
            headerList.add(headerKey);
            headerDisplayList.add(headerDisplay);
        }
        
        csvPrinter.printRecord(headerDisplayList);
    }

    @Override
    public void writeRows(ResultListWithColumns resultListWithColumns) throws Exception {
        if (resultListWithColumns == null || CollectionUtils.isEmpty(resultListWithColumns.getResultList())) {
            return;
        }

        if (headerList == null && CollectionUtils.isNotEmpty(resultListWithColumns.getColumns())) {
            headerList = new ArrayList<>();
            for (QueryColumn column : resultListWithColumns.getColumns()) {
                headerList.add(column.getName().toLowerCase());
            }
        }

        for (Map<String, Object> row : resultListWithColumns.getResultList()) {
            List<String> rowData = new ArrayList<>();
            if (headerList != null) {
                for (String header : headerList) {
                    Object value = row.get(header);
                    rowData.add(value == null ? "" : String.valueOf(value).toLowerCase());
                }
            }
            csvPrinter.printRecord(rowData);
        }
    }

    @Override
    public void flush() throws Exception {
        if (csvPrinter != null) {
            csvPrinter.flush();
        }
    }

    @Override
    public String getFileExtension() {
        return "csv";
    }

    @Override
    public void close() throws Exception {
        if (csvPrinter != null) {
            try {
                csvPrinter.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
