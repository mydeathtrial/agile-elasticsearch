package cloud.agileframework.elasticsearch.proxy.batch;

import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import cloud.agileframework.elasticsearch.proxy.insert.InsertResponse;
import lombok.Data;

import java.util.Map;

@Data
public class BatchResponse implements JdbcResponse {
    private int took;
    private boolean errors;
    private Map<Action, InsertResponse> items;

    public enum Action {
        create, index, delete, update
    }
}
