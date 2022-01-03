package cloud.agileframework.elasticsearch.proxy.create;

import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import lombok.Data;

@Data
public class CreateResponse implements JdbcResponse {
    private boolean acknowledged;
    private boolean shardsAcknowledged;
    private String index;

    @Override
    public int count() {
        return index == null ? 0 : 1;
    }
}
