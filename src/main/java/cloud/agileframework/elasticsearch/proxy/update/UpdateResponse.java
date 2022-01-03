package cloud.agileframework.elasticsearch.proxy.update;

import cloud.agileframework.common.annotation.Alias;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import lombok.Data;

@Data
public class UpdateResponse implements JdbcResponse {
    private Integer took;
    @Alias("time_out")
    private Boolean timeOut;
    private int total;
    private int updated;
    private int deleted;
    private int batches;
    @Alias("version_conflicts")
    private Integer versionConflicts;
    private Integer noops;
    private Retries retries;
    @Alias("throttled_millis")
    private Long throttledMillis;
    @Alias("requests_per_second")
    private Long requestsPerSecond;
    @Alias("throttled_until_millis")
    private Long throttledUntilMillis;
    private String[] failures;

    @Override
    public int count() {
        return this.getUpdated();
    }

    @Data
    public static class Retries {
        private Integer bulk;
        private Integer search;
    }
}
