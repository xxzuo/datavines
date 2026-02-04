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
package io.datavines.engine.local.connector.storage;

import io.datavines.engine.local.api.storage.StorageClient;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

public class LocalStorageClient implements StorageClient {

    private String basePath;

    @Override
    public void init(Map<String, Object> config) throws Exception {
        this.basePath = (String) config.getOrDefault("base_path", "/tmp/datavines/error_data");
        
        File baseDir = new File(basePath);
        if (!baseDir.exists()) {
            FileUtils.forceMkdir(baseDir);
        }
    }

    @Override
    public void uploadFile(String localFilePath, String remoteFilePath) throws Exception {
        Path source = Paths.get(localFilePath);
        Path target = Paths.get(remoteFilePath);
        
        File parentDir = target.getParent().toFile();
        if (!parentDir.exists()) {
            FileUtils.forceMkdir(parentDir);
        }
        
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void uploadFromInputStream(InputStream inputStream, String remoteFilePath) throws Exception {
        Path target = Paths.get(remoteFilePath);
        
        File parentDir = target.getParent().toFile();
        if (!parentDir.exists()) {
            FileUtils.forceMkdir(parentDir);
        }
        
        Files.copy(inputStream, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public String getFullPath(String fileName) {
        String separator = basePath.endsWith(File.separator) ? "" : File.separator;
        return basePath + separator + fileName;
    }

    @Override
    public boolean exists(String filePath) throws Exception {
        return Files.exists(Paths.get(filePath));
    }

    @Override
    public void delete(String filePath) throws Exception {
        Files.deleteIfExists(Paths.get(filePath));
    }

    @Override
    public String getStorageType() {
        return "localfile";
    }

    @Override
    public void close() throws Exception {
        // No resources to close for local storage
    }

    public String getBasePath() {
        return basePath;
    }
}
