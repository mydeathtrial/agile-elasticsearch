package cloud.agileframework.elasticsearch.proxy;

public interface JdbcResponse {
    default int count(){
        return 0;
    };

    default int[] counts() {
        return new int[0];
    }
}
