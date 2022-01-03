package cloud.agileframework.elasticsearch.proxy.insert;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import cloud.agileframework.elasticsearch.proxy.common.BatchUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Data
public class InsertHandler implements SqlParseProvider<InsertResponse, SQLInsertStatement> {
    public JdbcRequest of(SQLInsertStatement statement) {
        String body = BatchUtil.to(statement)
                .stream().map(JSONAware::toJSONString).collect(Collectors.joining("\n"));

        return JdbcRequest.builder()
                .url("_bulk")
                .method(RequestMethod.POST)
                .body(body+"\n")
                .handler(this)
                .build();
    }
}
