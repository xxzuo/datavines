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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import io.datavines.engine.local.api.storage.StorageClient;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

public class OSSStorageClient implements StorageClient {

    private OSS ossClient;
    private String endpoint;
    private String bucket;
    private String accessKeyId;
    private String accessKeySecret;
    private String objectKeyPrefix;

    @Override
    public void init(Map<String, Object> config) throws Exception {
        this.endpoint = (String) config.get("endpoint");
        this.bucket = (String) config.get("bucket");
        this.accessKeyId = (String) config.get("access_key_id");
        this.accessKeySecret = (String) config.get("access_key_secret");
        this.objectKeyPrefix = (String) config.getOrDefault("object_key_prefix", "datavines/error_data/");
        
        if (!objectKeyPrefix.endsWith("/")) {
            objectKeyPrefix = objectKeyPrefix + "/";
        }

        this.ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    @Override
    public void uploadFile(String localFilePath, String remoteFilePath) throws Exception {
        File file = new File(localFilePath);
        String objectKey = getObjectKey(remoteFilePath);
        ossClient.putObject(bucket, objectKey, file);
    }

    @Override
    public void uploadFromInputStream(InputStream inputStream, String remoteFilePath) throws Exception {
        String objectKey = getObjectKey(remoteFilePath);
        ossClient.putObject(bucket, objectKey, inputStream);
    }

    @Override
    public String getFullPath(String fileName) {
        return "oss://" + bucket + "/" + objectKeyPrefix + fileName;
    }

    @Override
    public boolean exists(String filePath) throws Exception {
        String objectKey = getObjectKey(filePath);
        return ossClient.doesObjectExist(bucket, objectKey);
    }

    @Override
    public void delete(String filePath) throws Exception {
        String objectKey = getObjectKey(filePath);
        ossClient.deleteObject(bucket, objectKey);
    }

    @Override
    public String getStorageType() {
        return "oss";
    }

    @Override
    public void close() throws Exception {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

    private String getObjectKey(String filePath) {
        if (filePath.startsWith("oss://")) {
            // Extract object key from full OSS path
            String path = filePath.substring(6);
            int slashIndex = path.indexOf("/");
            if (slashIndex > 0) {
                return path.substring(slashIndex + 1);
            }
        }
        
        if (filePath.startsWith(objectKeyPrefix)) {
            return filePath;
        }
        
        return objectKeyPrefix + filePath;
    }

    public String getBucket() {
        return bucket;
    }

    public String getObjectKeyPrefix() {
        return objectKeyPrefix;
    }
}
