package cloud.agileframework.elasticsearch.proxy;

import cloud.agileframework.common.util.http.RequestMethod;
import cloud.agileframework.elasticsearch.BaseStatement;
import cloud.agileframework.elasticsearch.proxy.batch.BatchHandler;
import cloud.agileframework.elasticsearch.proxy.create.CreateHandler;
import cloud.agileframework.elasticsearch.proxy.delete.DeleteHandler;
import cloud.agileframework.elasticsearch.proxy.insert.InsertHandler;
import cloud.agileframework.elasticsearch.proxy.select.OpendistroSelectHandler;
import cloud.agileframework.elasticsearch.proxy.select.SelectHandler;
import cloud.agileframework.elasticsearch.proxy.update.UpdateHandler;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import lombok.Builder;
import lombok.Data;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Vector;

@Data
@Builder
public class JdbcRequest {
    private static Logger logger = LoggerFactory.getLogger(JdbcRequest.class);
    @Builder.Default
    private static final SqlParseProvider[] handlers = new SqlParseProvider[]{
            new SelectHandler(),
            new OpendistroSelectHandler(),
            new CreateHandler(),
            new InsertHandler(),
            new UpdateHandler(),
            new DeleteHandler(),
            new BatchHandler()};
    private String url;
    private RequestMethod method;
    private String body;
    private SqlParseProvider handler;
    private String index;
    private BaseStatement statement;

    public static JdbcResponse send(String sql, BaseStatement baseStatement) throws SQLException, IOException {
        Vector<SendInfo> sendInfo = new Vector<>();

        JdbcRequest request;
        JdbcResponse response;
        SQLStatement sqlStatement = to(sql);
        boolean support = false;
        for (SqlParseProvider handler : handlers) {
            if (!handler.accept(sqlStatement)) {
                continue;
            }
            support = true;
            request = handler.of(sqlStatement);
            if (request != null) {
                SendInfo.SendInfoBuilder builder = SendInfo.builder().request(request);
                request.setStatement(baseStatement);
                try {
                    response = request.send();
                    if (response != null) {
                        builder.response(response);
                        break;
                    }
                } catch (Exception e) {
                    builder.e(e);
                } finally {
                    sendInfo.add(builder.build());
                }

            }
        }
        if (!support || sendInfo.isEmpty()) {
            throw new SQLFeatureNotSupportedException(sql);
        }
        SendInfo last = sendInfo.lastElement();
        if (last.getE() != null) {
            Exception exception = new Exception();
            for (SendInfo o : sendInfo) {
                JdbcRequest nextRequest = o.getRequest();
                logger.error("Send data:\n{} {}\n{}", nextRequest.getMethod(), nextRequest.getUrl(), nextRequest.getBody());
                logger.error("Send data fail", o.getE());
                exception.addSuppressed(o.getE());
            }
            throw new SQLException(exception);
        }
        JdbcRequest lastRequest = last.getRequest();
        logger.debug("Send data:\n{} {}\n{}", lastRequest.getMethod(), lastRequest.getUrl(), lastRequest.getBody());
        return last.getResponse();
    }

    @Data
    @Builder
    private static class SendInfo {
        private JdbcRequest request;
        private JdbcResponse response;
        private Exception e;
    }

    /**
     * ???sql????????????????????????????????????
     *
     * @param sql sql??????
     * @return ????????????
     * @throws SQLFeatureNotSupportedException ????????????sql??????
     */
    public static JdbcRequest of(String sql, BaseStatement baseStatement) throws SQLException {
        SQLStatement sqlStatement = to(sql);
        for (SqlParseProvider handler : handlers) {
            if (!handler.accept(sqlStatement)) {
                continue;
            }
            JdbcRequest request = handler.of(sqlStatement);
            if (request != null) {
                request.setStatement(baseStatement);
                return request;
            }
        }
        throw new SQLFeatureNotSupportedException(sql);
    }

    /**
     * ???sql????????????????????????????????????
     *
     * @param sqls sql????????????
     * @return ????????????
     * @throws SQLFeatureNotSupportedException ????????????sql??????
     */
    public static JdbcRequest of(List<SQLStatement> sqls, BaseStatement baseStatement) throws SQLException {
        for (SqlParseProvider handler : handlers) {
            try {
                JdbcRequest request = handler.of(sqls);
                if (request != null) {
                    request.setStatement(baseStatement);
                    return request;
                }
            } catch (SQLFeatureNotSupportedException ignored) {
            }
        }
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * sql??????????????????sql??????
     *
     * @param sql sql?????????
     * @return sql??????
     */
    public static SQLStatement to(String sql) {
        // ?????? SQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.elastic_search);

        // ??????Parser????????????AST?????????SQLStatement??????AST
        SQLStatement statement = parser.parseStatement();

        // ??????visitor?????????AST
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

    public JdbcResponse send() throws IOException, SQLException {
        Request request = new Request(getMethod().name(), getUrl());
        request.setJsonEntity(getBody());
        Response result = statement.getConnection().getRestClient().performRequest(request);
        return getHandler().toResponse(statement, result.getEntity().getContent());
    }
}
