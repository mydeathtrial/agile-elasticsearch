package cloud.agileframework.elasticsearch.proxy.common;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Locale;

public class BatchUtil {

    /**
     * 批量插入语句
     * @param sqlExpr 插入语句
     * @return { "create" : { "_index" : "test", "_type" : "_doc", "_id" : "3" } }
     */
    public static List<JSONObject> to(SQLInsertStatement sqlExpr) {
        List<JSONObject> list = Lists.newArrayList();
        
        JSONObject body = new JSONObject();
        JSONObject index = new JSONObject();
        body.put("create", index);
        index.put("_index", sqlExpr.getTableName().getSimpleName().toLowerCase(Locale.ROOT));
        index.put("_type", "_doc");

        List<SQLExpr> columns = sqlExpr.getColumns();
        sqlExpr.getValuesList().forEach(valuesClause -> {
            list.add(body);
            list.add(to(columns, valuesClause));
        });
        
        return list;
    }


    private static JSONObject to(List<SQLExpr> columns, SQLInsertStatement.ValuesClause valuesClause) {
        JSONObject body = new JSONObject();
        List<SQLExpr> values = valuesClause.getValues();

        for (int i = 0; i < values.size(); i++) {
            SQLExpr v = values.get(i);
            Object value;
            if (v instanceof SQLValuableExpr) {
                value = ((SQLValuableExpr) v).getValue();
            } else {
                value = SQLUtils.toSQLString(v);
            }
            String column = columns.get(i).toString();
            body.put(column, value);
        }
        return body;
    }

}
