package io.datavines.metric.plugin;

import io.datavines.common.enums.DataVinesDataType;
import io.datavines.metric.api.ConfigItem;
import io.datavines.metric.api.MetricDimension;
import io.datavines.metric.api.MetricType;
import io.datavines.metric.plugin.base.BaseSingleTable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class MultiColumnDuplicate extends BaseSingleTable {

    public MultiColumnDuplicate(){
        super();
        configMap.put("columnList",new ConfigItem("columnList", "列名", "columnList"));
        invalidateItemsSql = new StringBuilder("select ${columnList} from ${table}");
    }

    @Override
    public String getName() {
        return "multi_column_duplicate";
    }

    @Override
    public String getZhName() {
        return "多列重复值检查";
    }

    @Override
    public MetricDimension getDimension() {
        return MetricDimension.UNIQUENESS;
    }


    @Override
    public MetricType getType() {
        return MetricType.SINGLE_TABLE;
    }

    @Override
    public boolean isInvalidateItemsCanOutput() {
        return true;
    }

    @Override
    public Map<String, ConfigItem> getConfigMap() {
        return configMap;
    }

    @Override
    public List<DataVinesDataType> suitableType() {
        return Arrays.asList(DataVinesDataType.NUMERIC_TYPE, DataVinesDataType.STRING_TYPE, DataVinesDataType.DATE_TIME_TYPE);
    }
}
