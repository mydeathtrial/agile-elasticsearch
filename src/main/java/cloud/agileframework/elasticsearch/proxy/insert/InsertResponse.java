package cloud.agileframework.elasticsearch.proxy.insert;

import cloud.agileframework.common.annotation.Alias;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class InsertResponse implements JdbcResponse {
    private int took;
    private boolean errors = false;
    private List<Map<Type, CreatedInfo>> items;

    @Override
    public int count() {
        return errors ? items.size() : (int) items.stream().filter(i -> i.get(Type.create).error == null).count();
    }

    public enum Type {
        create
    }

    @Data
    public static class Shards {
        private int total;
        private int successful;
        private int failed;
    }

    @Data
    public static class CreatedInfo {
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
        private String error;
    }
}
