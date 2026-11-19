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
package io.datavines.engine.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The table reference parsed from a custom SQL statement.
 * Only the parts explicitly specified in the SQL are filled,
 * the absent parts are null and should fall back to the datasource configuration.
 */
@Getter
@AllArgsConstructor
public class CustomSqlTable {

    /**
     * database specified in the SQL, null when absent
     */
    private final String database;

    /**
     * schema specified in the SQL, null when absent
     */
    private final String schema;

    /**
     * table name, always present
     */
    private final String table;
}
