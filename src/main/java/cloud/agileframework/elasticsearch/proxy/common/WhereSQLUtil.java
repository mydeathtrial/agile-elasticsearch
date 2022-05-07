package cloud.agileframework.elasticsearch.proxy.common;

import cloud.agileframework.elasticsearch.proxy.SqlParseProvider;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLFeatureNotSupportedException;
import java.util.stream.Collectors;

public class WhereSQLUtil {
    public static JSONObject to(SqlParseProvider handler, SQLExpr op) throws SQLFeatureNotSupportedException {
        if(op == null){
            return null;
        }
        if (op instanceof SQLBinaryOpExpr) {
            return toSQLBinaryOpExpr(handler, (SQLBinaryOpExpr) op);
        } else if (op instanceof SQLInListExpr) {
            return toSQLInListExpr((SQLInListExpr) op);
        } else if (op instanceof SQLBetweenExpr) {
            return toSQLBetweenExpr((SQLBetweenExpr) op);
        } else {
            throw new SQLFeatureNotSupportedException(SQLUtils.toSQLString(op));
        }
    }

    public static JSONObject toSQLBinaryOpExpr(SqlParseProvider handler, SQLBinaryOpExpr op) throws SQLFeatureNotSupportedException {
        JSONObject node = new JSONObject();
        if (SQLBinaryOperator.BooleanAnd == op.getOperator()) {
            //And
            JSONObject bool = new JSONObject();
            JSONArray must = new JSONArray();
            node.put("bool", bool);
            must.add(to(handler, op.getLeft()));
            must.add(to(handler, op.getRight()));
            bool.put("must", must);

        } else if (SQLBinaryOperator.BooleanOr == op.getOperator()) {
            //Or
            JSONObject bool = new JSONObject();
            JSONArray must = new JSONArray();
            node.put("bool", bool);
            must.add(to(handler, op.getLeft()));
            must.add(to(handler, op.getRight()));
            bool.put("should", must);
        } else if (SQLBinaryOperator.NotEqual == op.getOperator() || SQLBinaryOperator.LessThanOrGreater == op.getOperator()) {
            //Or
            JSONObject bool = new JSONObject();
            JSONArray must = new JSONArray();
            node.put("bool", bool);
            must.add(to(handler, op.getLeft()));
            must.add(to(handler, op.getRight()));
            bool.put("must_not", must);
        } else {
            String column = SQLUtils.toSQLString(op.getLeft());
            String value = SQLUtils.toSQLString(op.getRight());
            String temp;
            switch (op.getOperator()) {
                case Equality:
                    temp = "{\n" +
                            "    \"term\": {\n" +
                            "        \"$_Key_.keyword\": {\n" +
                            "            \"value\": $_Value_\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
                    break;
                case GreaterThanOrEqual:
                    temp = "{\n" +
                            "    \"range\": {\n" +
                            "        \"$_Key_\": {\n" +
                            "            \"from\": $_Value_\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
                    break;
                case LessThanOrEqual:
                    temp = "{\n" +
                            "    \"range\": {\n" +
                            "        \"$_Key_\": {\n" +
                            "            \"to\": $_Value_\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
                    break;
                case GreaterThan:
                    temp = "{\n" +
                            "    \"range\": {\n" +
                            "        \"$_Key_\": {\n" +
                            "            \"gt\": $_Value_,\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
                    break;
                case LessThan:
                    temp = "{\n" +
                            "    \"range\": {\n" +
                            "        \"$_Key_\": {\n" +
                            "            \"lt\": $_Value_,\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
                    break;
                case Like:
                    value = value.replace("%", "*");
                    temp = "{\n" +
                            "    \"wildcard\": {\n" +
                            "        \"$_Key_\": $_Value_\n" +
                            "    }\n" +
                            "}";
                    break;
                case IsNot:
                    temp = "{\n" +
                            "        \"bool\": {\n" +
                            "            \"must\": {\n" +
                            "                \"exists\": {\n" +
                            "                    \"field\": \"$_Key_\"\n" +
                            "                }\n" +
                            "            }\n" +
                            "        }\n" +
                            "    }";
                    break;
                case Is:
                    temp = "{\n" +
                            "        \"bool\": {\n" +
                            "            \"must_not\": {\n" +
                            "                \"exists\": {\n" +
                            "                    \"field\": \"$_Key_\"\n" +
                            "                }\n" +
                            "            }\n" +
                            "        }\n" +
                            "    }";
                    break;
                default:
                    throw new SQLFeatureNotSupportedException(op.getOperator().name);
            }

            return JSON.parseObject(temp.replace("$_Key_", column).replace("$_Value_", value));
        }
        return node;
    }

    private static JSONObject toSQLBetweenExpr(SQLBetweenExpr op) {
        String column = SQLUtils.toSQLString(op.getTestExpr());
        String begin = SQLUtils.toSQLString(op.getBeginExpr());
        String end = SQLUtils.toSQLString(op.getEndExpr());
        String temp = "{\n" +
                "    \"range\": {\n" +
                "        \"$_Key_\": {\n" +
                "            \"from\": $_Begin_,\n" +
                "            \"to\": $_End_,\n" +
                "        }\n" +
                "    }\n" +
                "}";
        return JSON.parseObject(temp.replace("$_Key_", column).replace("$_Begin_", begin).replace("$_End_", end));
    }

    public static JSONObject toSQLInListExpr(SQLInListExpr in) {
        String column = SQLUtils.toSQLString(in.getExpr());
        String value = in.getTargetList().stream().map(SQLUtils::toSQLString).collect(Collectors.joining(","));
        String temp = "{\n" +
                "    \"terms\": {\n" +
                "        \"$_Key_\": [$_Value_]\n" +
                "    }\n" +
                "}";
        return JSON.parseObject(temp.replace("$_Key_", column).replace("$_Value_", value));
    }
}
