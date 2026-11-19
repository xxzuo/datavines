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

public class PostgreSqlUrlUtils {

    private PostgreSqlUrlUtils() {
    }

    /**
     * Append currentSchema to the JDBC url when a schema is configured and the
     * url does not already specify one. The PostgreSQL JDBC driver turns this
     * property into SET search_path, so unqualified table names in custom SQL
     * resolve against the configured schema instead of only the default public.
     * An explicit currentSchema in the url (e.g. from datasource properties)
     * always takes precedence.
     */
    public static String appendCurrentSchema(String url, String schema) {
        if (StringUtils.isEmpty(schema) || containsCurrentSchema(url)) {
            return url;
        }

        return url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema;
    }

    private static boolean containsCurrentSchema(String url) {
        int queryIndex = url.indexOf('?');
        if (queryIndex < 0) {
            return false;
        }

        for (String param : url.substring(queryIndex + 1).split("&")) {
            if (param.toLowerCase().startsWith("currentschema=")) {
                return true;
            }
        }

        return false;
    }
}
