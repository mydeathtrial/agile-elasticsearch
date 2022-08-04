package cloud.agileframework.elasticsearch.proxy.batch;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import cloud.agileframework.elasticsearch.proxy.common.BatchUtil;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastjson.JSONAware;
import lombok.Data;

import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class BatchHandler implements SqlParseProvider<BatchResponse, SQLInsertStatement> {
    @Override
    public JdbcRequest of(List<SQLInsertStatement> statement) throws SQLFeatureNotSupportedException {
        String body = statement.stream()
                .flatMap(sql -> BatchUtil.to(sql).stream())
                .map(JSONAware::toJSONString)
                .collect(Collectors.joining("\n"));

        return JdbcRequest.builder()
                .url("_bulk")
                .method(RequestMethod.POST)
                .body(body + "\n")
                .handler(this)
                .build();
    }
}
