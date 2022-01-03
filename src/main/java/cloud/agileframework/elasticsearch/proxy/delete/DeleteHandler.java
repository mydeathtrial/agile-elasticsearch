package cloud.agileframework.elasticsearch.proxy.delete;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import cloud.agileframework.elasticsearch.proxy.common.WhereSQLUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLFeatureNotSupportedException;

public class DeleteHandler implements SqlParseProvider<DeleteResponse, SQLDeleteStatement> {
    @Override
    public JdbcRequest of(SQLDeleteStatement statement) throws SQLFeatureNotSupportedException {
        SQLExpr where = statement.getWhere();
        String index = statement.getTableName().toString();

        JSONObject body = new JSONObject();
        body.put("query", WhereSQLUtil.to(this, where));
        return JdbcRequest.builder()
                .handler(this)
                .method(RequestMethod.POST)
                .url(index + "/_delete_by_query")
                .index(index)
                .body(body.toJSONString())
                .build();
    }
}
