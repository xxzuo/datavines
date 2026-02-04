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
package io.datavines.connector.plugin;

import io.datavines.common.utils.StringUtils;
import io.datavines.connector.api.Dialect;
import io.datavines.connector.api.entity.ResultList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LocalFileDialect implements Dialect {

    @Override
    public String getDriver() {
        return null;
    }

    @Override
    public String getColumnPrefix() {
        return null;
    }

    @Override
    public String getColumnSuffix() {
        return null;
    }

    @Override
    public List<String> getExcludeDatabases() {
        return Collections.emptyList();
    }

    @Override
    public boolean supportToBeErrorDataStorage() {
        return true;
    }

    @Override
    public String getErrorDataScript(Map<String, String> configMap) {
        String basePath = configMap.get("base_path");
        String errorDataFileName = configMap.get("error_data_file_name");
        String fileFormat = configMap.getOrDefault("file_format", "csv");
        
        if (StringUtils.isNotEmpty(basePath) && StringUtils.isNotEmpty(errorDataFileName)) {
            String separator = basePath.endsWith("/") || basePath.endsWith("\\") ? "" : "/";
            return basePath + separator + errorDataFileName + "." + fileFormat.toLowerCase();
        }
        return null;
    }

    @Override
    public String getValidateResultDataScript(Map<String, String> configMap) {
        String basePath = configMap.get("base_path");
        String executionId = configMap.get("execution_id");
        String fileFormat = configMap.getOrDefault("file_format", "csv");
        
        if (StringUtils.isNotEmpty(basePath) && StringUtils.isNotEmpty(executionId)) {
            String separator = basePath.endsWith("/") || basePath.endsWith("\\") ? "" : "/";
            return basePath + separator + executionId + "/validate_result." + fileFormat.toLowerCase();
        }
        return null;
    }

    @Override
    public ResultList getPageFromResultSet(Statement sourceConnectionStatement, ResultSet rs, String sourceTable, int start, int end) throws SQLException {
        return null;
    }
}
