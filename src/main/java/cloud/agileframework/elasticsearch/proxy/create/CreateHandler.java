package cloud.agileframework.elasticsearch.proxy.create;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class CreateHandler implements SqlParseProvider<CreateResponse, SQLCreateTableStatement> {
    public JdbcRequest of(SQLCreateTableStatement statement) {
        String index = statement.getTableName().toLowerCase(Locale.ROOT);
        Map<String, JSONObject> properties = statement.getColumnDefinitions()
                .stream()
                .collect(Collectors.toMap(SQLColumnDefinition::getColumnName, c -> {
                    String type = c.getDataType().getName();
                    SQLExpr comment = c.getComment();
                    JSONObject property = new JSONObject();
                    property.put("type", type);

                    if (comment != null) {
                        try {
                            JSONObject attr = JSON.parseObject(((SQLCharExpr) comment).getValue().toString());
                            property.putAll(attr);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    return property;
                }));


        JSONObject createTableJson = new JSONObject();
        JSONObject mappings = new JSONObject();
        mappings.put("properties", properties);
        createTableJson.put("mappings", mappings);
        SQLExpr comment = statement.getComment();
        if (comment != null) {
            try {
                JSONObject createTableConfigJson = JSON.parseObject(((SQLCharExpr) comment).getValue().toString());
                createTableJson.putAll(createTableConfigJson);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return JdbcRequest.builder()
                .handler(this)
                .url(index)
                .method(RequestMethod.PUT)
                .index(index)
                .body(createTableJson.toJSONString())
                .build();
    }
}
