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

import io.datavines.engine.local.api.writer.ErrorDataFileWriter;

public class ErrorDataWriterFactory {

    public static final String FORMAT_CSV = "csv";
    public static final String FORMAT_JSON = "json";
    public static final String FORMAT_PARQUET = "parquet";

    public static ErrorDataFileWriter createWriter(String fileFormat) {
        if (fileFormat == null) {
            fileFormat = FORMAT_CSV;
        }
        
        switch (fileFormat.toLowerCase()) {
            case FORMAT_JSON:
                return new JsonErrorDataWriter();
            case FORMAT_PARQUET:
                return new ParquetErrorDataWriter();
            case FORMAT_CSV:
            default:
                return new CsvErrorDataWriter();
        }
    }
}
