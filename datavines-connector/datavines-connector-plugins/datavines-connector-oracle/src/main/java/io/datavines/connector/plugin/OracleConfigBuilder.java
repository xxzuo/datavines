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

import io.datavines.common.param.form.Validate;
import io.datavines.common.param.form.type.InputParam;

public class OracleConfigBuilder extends JdbcConfigBuilder{
    @Override
    protected InputParam getSchemaInput(boolean isEn) {
        return getInputParam("schema",
                isEn ? "schema" : "模式",
                isEn ? "please enter schema" : "请填入模式", 1,
                Validate.newBuilder().setRequired(true).setMessage(isEn ? "please enter schema" : "请填入模式").build(), null);
    }

    @Override
    protected InputParam getPropertiesInput(boolean isEn) {
        return getInputParam("properties",
                isEn ? "properties" : "参数",
                isEn ? "please enter properties,like key=value&key1=value1" : "请填入参数，格式为key=value&key1=value1", 2, null,
                null);
    }

}
