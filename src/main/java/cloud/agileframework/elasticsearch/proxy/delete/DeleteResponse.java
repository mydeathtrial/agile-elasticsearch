package cloud.agileframework.elasticsearch.proxy.delete;

import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import cloud.agileframework.elasticsearch.proxy.update.UpdateResponse;

public class DeleteResponse extends UpdateResponse implements JdbcResponse {
    @Override
    public int count() {
        return getDeleted();
    }
}
