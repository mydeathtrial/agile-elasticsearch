package cloud.agileframework.elasticsearch.proxy.select;

import cloud.agileframework.elasticsearch.AgileResultSet;
import cloud.agileframework.elasticsearch.proxy.BaseResponse;
import com.google.common.collect.Lists;
import lombok.Data;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;

@Data
public class OpendistroSelectResponse extends BaseResponse {
    private List<AgileResultSet.Column> schema;
    private List<List<Object>> datarows = Lists.newArrayList();
    private int total;
    private int size;
    private int status;

    @Override
    public int count() {
        return total;
    }

    @Override
    public ResultSet resultSet() {
        return new AgileResultSet(getStatement(), schema, datarows, LoggerFactory.getLogger(OpendistroSelectResponse.class));
    }
}
