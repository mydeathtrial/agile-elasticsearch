package cloud.agileframework.elasticsearch.proxy.select;

import cloud.agileframework.elasticsearch.AgileResultSet;
import cloud.agileframework.elasticsearch.proxy.BaseResponse;
import com.google.common.collect.Lists;
import lombok.Data;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;

@Data
public class SelectResponse extends BaseResponse {
    private List<AgileResultSet.Column> columns;
    private List<List<Object>> rows = Lists.newArrayList();

    private String cursor;

    @Override
    public int count() {
        return rows.size();
    }

    @Override
    public ResultSet resultSet() {
        return new AgileResultSet(getStatement(), columns, rows, LoggerFactory.getLogger(SelectResponse.class));
    }
}
