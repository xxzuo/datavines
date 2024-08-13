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
import io.datavines.common.datasource.jdbc.JdbcConnectionInfo;
import io.datavines.common.utils.StringUtils;
import io.datavines.connector.plugin.MysqlDataSourceInfo;

public class DorisCatalogDataSourceInfo extends MysqlDataSourceInfo {
    public DorisCatalogDataSourceInfo(JdbcConnectionInfo jdbcConnectionInfo) {
        super(jdbcConnectionInfo);
    }

    @Override
    public String getJdbcUrl() {
        StringBuilder jdbcUrl = new StringBuilder(getAddress());
        appendCatalogOrDatabase(jdbcUrl);
        appendProperties(jdbcUrl);

        return jdbcUrl.toString();
    }

    private void appendCatalogOrDatabase(StringBuilder jdbcUrl) {
        if (StringUtils.isNotEmpty(getCatalog())) {
            if (getAddress().lastIndexOf('/') != (jdbcUrl.length() - 1)) {
                jdbcUrl.append("/");
            }
            jdbcUrl.append(getCatalog());

            if (StringUtils.isNotEmpty(getDatabase())) {
                if (getAddress().lastIndexOf('.') != (jdbcUrl.length() - 1)) {
                    jdbcUrl.append(".");
                }
                jdbcUrl.append(getDatabase());
            }

        } else {
            if (StringUtils.isNotEmpty(getDatabase())) {
                if (getAddress().lastIndexOf('/') != (jdbcUrl.length() - 1)) {
                    jdbcUrl.append("/");
                }
                jdbcUrl.append(getDatabase());
            }
        }
    }
}
