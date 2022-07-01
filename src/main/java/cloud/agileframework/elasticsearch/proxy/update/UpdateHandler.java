package cloud.agileframework.elasticsearch.proxy.update;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import cloud.agileframework.elasticsearch.proxy.common.WhereSQLUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateHandler implements SqlParseProvider<UpdateResponse, SQLUpdateStatement> {
    @Override
    public JdbcRequest of(SQLUpdateStatement statement) throws SQLFeatureNotSupportedException {
        SQLExpr where = statement.getWhere();
        String index = statement.getTableName().toString();

        JSONObject body = new JSONObject();
        body.put("script", to(statement.getItems()));
        body.put("query", WhereSQLUtil.to(where));
        return JdbcRequest.builder()
                .handler(this)
                .method(RequestMethod.POST)
                .url(index + "/_update_by_query")
                .index(index)
                .body(body.toJSONString())
                .build();
    }

    public JSONObject to(List<SQLUpdateSetItem> items) {
        JSONObject script = new JSONObject();
        String inline = items.stream().map(item -> {
            String columnName = SQLUtils.toSQLString(item.getColumn());
            return "ctx._source." + columnName + "=" + "params." + columnName;
        }).collect(Collectors.joining(";"));
        script.put("inline", inline);

        JSONObject params = new JSONObject();
        items.forEach(item -> {
            String column = SQLUtils.toSQLString(item.getColumn());
            SQLExpr v = item.getValue();
            Object value = v instanceof SQLValuableExpr?((SQLValuableExpr) v).getValue():SQLUtils.toSQLString(v);
            params.put(column, value);
        });
        script.put("params", params);
        script.put("lang", "painless");
        return script;
    }


}
