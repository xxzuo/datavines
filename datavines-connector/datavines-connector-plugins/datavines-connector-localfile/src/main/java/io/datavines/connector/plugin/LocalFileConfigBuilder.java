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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.common.CommonConstants;
import io.datavines.common.param.form.ParamsOptions;
import io.datavines.common.param.form.PluginParams;
import io.datavines.common.param.form.PropsType;
import io.datavines.common.param.form.Validate;
import io.datavines.common.param.form.props.InputParamsProps;
import io.datavines.common.param.form.type.InputParam;
import io.datavines.common.param.form.type.SelectParam;
import io.datavines.connector.api.ConfigBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LocalFileConfigBuilder implements ConfigBuilder {

    @Override
    public String build(boolean isEn) {
        return buildErrorDataStorage(isEn);
    }

    @Override
    public String buildErrorDataStorage(boolean isEn) {
        List<PluginParams> params = new ArrayList<>();
        
        params.add(getBasePathInput(isEn));
        params.add(getFileFormatSelect(isEn));
        params.add(getColumnSeparatorInput(isEn));

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;

        try {
            result = mapper.writeValueAsString(params);
        } catch (JsonProcessingException e) {
            log.error("json parse error : ", e);
        }

        return result;
    }

    private InputParam getBasePathInput(boolean isEn) {
        return InputParam
                .newBuilder("base_path", isEn ? "Base Path" : "基础路径")
                .addValidate(Validate.newBuilder()
                        .setRequired(true)
                        .setMessage(isEn ? "Please enter base path" : "请填入基础路径")
                        .build())
                .setProps(new InputParamsProps().setDisabled(false))
                .setSize(CommonConstants.SMALL)
                .setType(PropsType.TEXT)
                .setRows(1)
                .setPlaceholder(isEn ? "Please enter base path, e.g. /tmp/datavines/error_data" : "请填入基础路径，如 /tmp/datavines/error_data")
                .setValue("/tmp/datavines/error_data")
                .setEmit(null)
                .build();
    }

    private SelectParam getFileFormatSelect(boolean isEn) {
        return SelectParam
                .newBuilder("file_format", isEn ? "File Format" : "文件格式")
                .addParamsOptions(new ParamsOptions("CSV", "csv", false))
                .addParamsOptions(new ParamsOptions("JSON", "json", false))
                .addParamsOptions(new ParamsOptions("Parquet", "parquet", false))
                .addValidate(Validate.newBuilder()
                        .setRequired(true)
                        .setMessage(isEn ? "Please select file format" : "请选择文件格式")
                        .build())
                .setValue("csv")
                .setSize(CommonConstants.SMALL)
                .build();
    }

    private InputParam getColumnSeparatorInput(boolean isEn) {
        return InputParam
                .newBuilder("column_separator", isEn ? "Column Separator" : "列分隔符")
                .setProps(new InputParamsProps().setDisabled(false))
                .setSize(CommonConstants.SMALL)
                .setType(PropsType.TEXT)
                .setRows(1)
                .setPlaceholder(isEn ? "Column separator for CSV format, default is comma" : "CSV格式的列分隔符，默认为逗号")
                .setValue(",")
                .setEmit(null)
                .build();
    }
}
