package cloud.agileframework.elasticsearch.proxy.select;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastjson.JSONObject;

import java.util.List;


public class SelectHandler implements SqlParseProvider<SelectResponse, SQLSelectStatement> {
    public JdbcRequest of(SQLSelectStatement statement) {
        JSONObject json = new JSONObject();
        
        json.put("query", statement.toString());

        return JdbcRequest.builder()
                .url("_sql")
                .method(RequestMethod.POST)
                .body(json.toJSONString())
                .handler(this)
                .build();
    }
}
