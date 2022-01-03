package cloud.agileframework.elasticsearch.proxy;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.proxy.batch.BatchHandler;
import cloud.agileframework.elasticsearch.proxy.create.CreateHandler;
import cloud.agileframework.elasticsearch.proxy.delete.DeleteHandler;
import cloud.agileframework.elasticsearch.proxy.insert.InsertHandler;
import cloud.agileframework.elasticsearch.proxy.update.UpdateHandler;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Data;

import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@Builder
public class JdbcRequest {
    @Builder.Default
    private static Set<SqlParseProvider<?, ?>> handlers = Sets.newHashSet(new CreateHandler(),
            new InsertHandler(),
            new UpdateHandler(),
            new DeleteHandler(),
            new BatchHandler());
    private String url;
    private RequestMethod method;
    private String body;
    private SqlParseProvider handler;
    private String index;

    /**
     * 将sql转换为语法对应的请求信息
     *
     * @param sql sql语句
     * @return 请求信息
     * @throws SQLFeatureNotSupportedException 不支持的sql语法
     */
    public static JdbcRequest of(String sql) throws SQLFeatureNotSupportedException {
        SQLStatement sqlStatement = to(sql);
        for (SqlParseProvider handler : handlers) {
            if (!handler.accept(sqlStatement)) {
                continue;
            }
            JdbcRequest request = handler.of(sqlStatement);
            if (request != null) {
                return request;
            }
        }
        throw new SQLFeatureNotSupportedException(sql);
    }

    /**
     * 将sql转换为语法对应的请求信息
     *
     * @param sqls sql语句集合
     * @return 请求信息
     * @throws SQLFeatureNotSupportedException 不支持的sql语法
     */
    public static JdbcRequest of(List<SQLStatement> sqls) throws SQLFeatureNotSupportedException {
        for (SqlParseProvider handler : handlers) {
           try {
               JdbcRequest request = handler.of(sqls);
               if (request != null) {
                   return request;
               }
           }catch (SQLFeatureNotSupportedException ignored){
           }
        }
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * sql字符串转换成sql对象
     *
     * @param sql sql字符串
     * @return sql对象
     */
    public static SQLStatement to(String sql) {
        // 新建 SQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.elastic_search);

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(DbType.elastic_search);
        statement.accept(visitor);
        return statement;
    }

    public String getUrl() {
        if (!url.startsWith("/")) {
            url = "/" + url;
        }
        return url;
    }
}
