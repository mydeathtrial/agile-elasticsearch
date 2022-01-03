package cloud.agileframework.elasticsearch.proxy.insert;

import cloud.agileframework.common.annotation.Alias;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import lombok.Data;

@Data
public class InsertResponse implements JdbcResponse {
    @Alias("_index")
    private String index;
    @Alias("_type")
    private String type;
    @Alias("_id")
    private String id;
    @Alias("_version")
    private int version;
    private String result;
    @Alias("_shards")
    private Shards shards;
    @Alias("_seq_no")
    private int seqNo;
    @Alias("_primary_term")
    private int primaryTerm;
    @Override
    public int count() {
        return "created".equals(result)?1:0;
    }

    @Data
    public static class Shards {
        private int total;
        private int successful;
        private int failed;
    }
}
