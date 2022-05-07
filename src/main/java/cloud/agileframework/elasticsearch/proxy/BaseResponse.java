package cloud.agileframework.elasticsearch.proxy;

import cloud.agileframework.elasticsearch.BaseStatement;

import java.sql.Statement;

public abstract class BaseResponse implements JdbcResponse {
    private BaseStatement statement;

    public BaseStatement getStatement() {
        return statement;
    }

    public void setStatement(BaseStatement statement) {
        this.statement = statement;
    }
}
