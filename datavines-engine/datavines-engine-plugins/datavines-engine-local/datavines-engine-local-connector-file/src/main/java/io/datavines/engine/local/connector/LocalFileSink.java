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
package io.datavines.engine.local.connector;

import io.datavines.common.config.CheckResult;
import io.datavines.common.config.Config;
import io.datavines.common.config.enums.SinkType;
import io.datavines.common.utils.ParameterUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.connector.api.ConnectorFactory;
import io.datavines.connector.api.TypeConverter;
import io.datavines.engine.api.env.RuntimeEnvironment;
import io.datavines.engine.local.api.LocalRuntimeEnvironment;
import io.datavines.engine.local.api.LocalSink;
import io.datavines.connector.api.entity.ResultList;
import io.datavines.connector.api.entity.ResultListWithColumns;
import io.datavines.engine.local.api.storage.StorageClient;
import io.datavines.engine.local.api.writer.ErrorDataFileWriter;
import io.datavines.engine.local.api.utils.FileUtils;
import io.datavines.engine.local.api.utils.LoggerFactory;
import io.datavines.engine.local.connector.storage.LocalStorageClient;
import io.datavines.engine.local.connector.storage.StorageClientFactory;
import io.datavines.engine.local.connector.writer.ErrorDataWriterFactory;
import io.datavines.connector.api.utils.SqlUtils;
import io.datavines.spi.PluginLoader;

import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static io.datavines.common.ConfigConstants.*;
import static io.datavines.engine.api.EngineConstants.PLUGIN_TYPE;

public class LocalFileSink implements LocalSink {

    private final Logger log = LoggerFactory.getLogger(LocalFileSink.class);

    private Config config = new Config();

    @Override
    public void output(List<ResultList> resultList, LocalRuntimeEnvironment env) throws Exception{

        Map<String,String> inputParameter = new HashMap<>();
        setExceptedValue(config, resultList, inputParameter);

        String validateResultDataDir = config.getString("data_dir") + File.separator + config.getString(JOB_EXECUTION_ID);

        switch (SinkType.of(config.getString(PLUGIN_TYPE))){
            case ERROR_DATA:
                sinkErrorData(env);
                SqlUtils.dropView(config.getString(INVALIDATE_ITEMS_TABLE), env.getSourceConnection().getConnection());
                break;
            case ACTUAL_VALUE:
            case VALIDATE_RESULT:
                String sql = config.getString(SQL);
                sql = ParameterUtils.convertParameterPlaceholders(sql, inputParameter);
                FileUtils.writeToLocal(parseSqlToList(sql), validateResultDataDir,config.getString(PLUGIN_TYPE).toLowerCase());
                log.info("execute " + config.getString(PLUGIN_TYPE) + " output sql : {}", sql);
                break;
            default:
                break;
        }
    }

    @Override
    public void prepare(RuntimeEnvironment env) {

    }

    @Override
    public void setConfig(Config config) {
        if(config != null) {
            this.config = config;
        }
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    private static List<String> parseSqlToList(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return null;
        }

        String[] splitValues = sql.split("ON DUPLICATE KEY UPDATE");
        if (splitValues.length >= 1) {
            sql = splitValues[0];
        }

        String[] values = sql.substring(sql.indexOf("("))
                .replaceAll("\\(","")
                .replaceAll("\\)","")
                .replaceAll("`","")
                .replaceAll("'","")
                .split("VALUES");

        return Arrays.asList(values);
    }

    private void sinkErrorData(LocalRuntimeEnvironment env) throws SQLException {
        String storageType = config.getString("storage_type");
        String fileFormat = config.getString("file_format");
        
        // Use new multi-format/storage implementation if storage_type or file_format is specified
        if (StringUtils.isNotEmpty(storageType) || StringUtils.isNotEmpty(fileFormat)) {
            sinkErrorDataWithFormat(env);
        } else {
            // Fallback to legacy CSV-only local file implementation
            sinkErrorDataLegacy(env);
        }
    }

    private void sinkErrorDataWithFormat(LocalRuntimeEnvironment env) throws SQLException {
        String columnSeparator = config.getString(COLUMN_SEPARATOR);
        String outputTable = config.getString(INVALIDATE_ITEMS_TABLE);
        
        if (!TRUE.equals(config.getString(INVALIDATE_ITEM_CAN_OUTPUT)) || StringUtils.isEmptyOrNullStr(outputTable)) {
            return;
        }

        String storageType = config.getString("storage_type");
        if (StringUtils.isEmpty(storageType)) {
            storageType = StorageClientFactory.TYPE_LOCAL;
        }
        
        String fileFormat = config.getString("file_format");
        if (StringUtils.isEmpty(fileFormat)) {
            fileFormat = ErrorDataWriterFactory.FORMAT_CSV;
        }

        int count = 0;
        Statement statement = env.getSourceConnection().getConnection().createStatement();
        ResultSet countResultSet = statement.executeQuery("SELECT COUNT(1) FROM " + outputTable);
        if (countResultSet.next()) {
            count = countResultSet.getInt(1);
        }
        countResultSet.close();

        if (count <= 0) {
            return;
        }

        String srcConnectorType = config.getString(SRC_CONNECTOR_TYPE);
        TypeConverter typeConverter = PluginLoader.getPluginLoader(ConnectorFactory.class)
                .getOrCreatePlugin(srcConnectorType).getTypeConverter();

        count = Math.min(count, 10000);
        int pageSize = 1000;
        int totalPage = count / pageSize + (count % pageSize > 0 ? 1 : 0);

        StorageClient storageClient = null;
        ErrorDataFileWriter writer = null;
        String tempFilePath = null;

        try {
            // Build storage config from connector config
            Map<String, Object> storageConfig = buildStorageConfig();
            storageClient = StorageClientFactory.createAndInitClient(storageType, storageConfig);

            // Create file writer
            Map<String, Object> writerConfig = new HashMap<>();
            writerConfig.put("column_separator", columnSeparator != null ? columnSeparator : ",");
            writerConfig.put("type_converter", typeConverter);
            
            writer = ErrorDataWriterFactory.createWriter(fileFormat);
            writer.init(writerConfig);

            // Determine file path
            String errorDataFileName = config.getString(ERROR_DATA_FILE_NAME);
            String fileName = errorDataFileName + "." + writer.getFileExtension();
            
            String filePath;
            if (StorageClientFactory.TYPE_OSS.equals(storageType)) {
                // For OSS, write to temp local file first, then upload
                tempFilePath = System.getProperty("java.io.tmpdir") + File.separator + 
                        "datavines_" + System.currentTimeMillis() + "_" + fileName;
                filePath = tempFilePath;
            } else {
                // For local storage, write directly to target path
                String errorDataDir = config.getString(ERROR_DATA_DIR);
                if (StringUtils.isEmpty(errorDataDir) && storageClient instanceof LocalStorageClient) {
                    errorDataDir = ((LocalStorageClient) storageClient).getBasePath();
                }
                File dir = new File(errorDataDir);
                if (!dir.exists()) {
                    org.apache.commons.io.FileUtils.forceMkdir(dir);
                }
                filePath = errorDataDir + File.separator + fileName;
            }

            writer.open(filePath);

            // Query and write data
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + outputTable);
            
            for (int i = 0; i < totalPage; i++) {
                int start = i * pageSize;
                int end = (i + 1) * pageSize;

                ResultListWithColumns resultList = SqlUtils.getListWithHeaderFromResultSet(resultSet, start, end);
                
                if (i == 0) {
                    writer.writeHeader(resultList);
                }
                writer.writeRows(resultList);
            }
            resultSet.close();

            writer.flush();
            writer.close();
            writer = null;

            // Upload to OSS if needed
            if (StorageClientFactory.TYPE_OSS.equals(storageType) && tempFilePath != null) {
                String remotePath = storageClient.getFullPath(fileName);
                storageClient.uploadFile(tempFilePath, remotePath);
                log.info("Uploaded error data to OSS: {}", remotePath);
            } else {
                log.info("Written error data to local file: {}", filePath);
            }

        } catch (Exception e) {
            log.error("Failed to sink error data with format", e);
            throw new SQLException("Failed to sink error data: " + e.getMessage(), e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    log.warn("Failed to close writer", e);
                }
            }
            if (storageClient != null) {
                try {
                    storageClient.close();
                } catch (Exception e) {
                    log.warn("Failed to close storage client", e);
                }
            }
            // Clean up temp file
            if (tempFilePath != null) {
                try {
                    Files.deleteIfExists(Paths.get(tempFilePath));
                } catch (Exception e) {
                    log.warn("Failed to delete temp file: {}", tempFilePath, e);
                }
            }
        }
    }

    private Map<String, Object> buildStorageConfig() {
        Map<String, Object> storageConfig = new HashMap<>();
        
        // Local storage config
        String basePath = config.getString("base_path");
        if (StringUtils.isNotEmpty(basePath)) {
            storageConfig.put("base_path", basePath);
        } else {
            String errorDataDir = config.getString(ERROR_DATA_DIR);
            if (StringUtils.isNotEmpty(errorDataDir)) {
                storageConfig.put("base_path", errorDataDir);
            }
        }
        
        // OSS config
        String endpoint = config.getString("endpoint");
        if (StringUtils.isNotEmpty(endpoint)) {
            storageConfig.put("endpoint", endpoint);
        }
        
        String bucket = config.getString("bucket");
        if (StringUtils.isNotEmpty(bucket)) {
            storageConfig.put("bucket", bucket);
        }
        
        String accessKeyId = config.getString("access_key_id");
        if (StringUtils.isNotEmpty(accessKeyId)) {
            storageConfig.put("access_key_id", accessKeyId);
        }
        
        String accessKeySecret = config.getString("access_key_secret");
        if (StringUtils.isNotEmpty(accessKeySecret)) {
            storageConfig.put("access_key_secret", accessKeySecret);
        }
        
        String objectKeyPrefix = config.getString("object_key_prefix");
        if (StringUtils.isNotEmpty(objectKeyPrefix)) {
            storageConfig.put("object_key_prefix", objectKeyPrefix);
        }
        
        return storageConfig;
    }

    private void sinkErrorDataLegacy(LocalRuntimeEnvironment env) throws SQLException{
        String columnSeparator = config.getString(COLUMN_SEPARATOR);
        String outputTable = config.getString(INVALIDATE_ITEMS_TABLE);
        if (TRUE.equals(config.getString(INVALIDATE_ITEM_CAN_OUTPUT)) && !StringUtils.isEmptyOrNullStr(outputTable)) {
            int count = 0;
            //执行统计行数语句
            Statement statement = env.getSourceConnection().getConnection().createStatement();
            ResultSet countResultSet = statement.executeQuery("SELECT COUNT(1) FROM " + outputTable);
            if (countResultSet.next()) {
                count = countResultSet.getInt(1);
            }

            String srcConnectorType = config.getString(SRC_CONNECTOR_TYPE);
            TypeConverter typeConverter = PluginLoader.getPluginLoader(ConnectorFactory.class).getOrCreatePlugin(srcConnectorType).getTypeConverter();
            if (count > 0) {
                count = Math.min(count, 10000);
                //根据行数进行分页查询。分批写到文件里面
                int pageSize = 1000;
                int totalPage = count/pageSize + (count%pageSize>0 ? 1:0);

                ResultSet resultSet = statement.executeQuery("SELECT * FROM " + outputTable);

                for (int i=0; i<totalPage; i++) {
                    int start = i * pageSize;
                    int end = (i+1) * pageSize;

                    ResultListWithColumns resultList = SqlUtils.getListWithHeaderFromResultSet(resultSet,  start, end);
                    //执行文件下载到本地
                    FileUtils.writeToLocal(resultList,
                            config.getString(ERROR_DATA_DIR),
                            config.getString(ERROR_DATA_FILE_NAME),
                            i==0,
                            typeConverter,
                            columnSeparator);
                }

                resultSet.close();
            }
        }
    }
}
