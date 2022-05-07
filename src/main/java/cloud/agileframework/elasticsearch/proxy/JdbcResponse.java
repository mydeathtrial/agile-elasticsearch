package cloud.agileframework.elasticsearch.proxy;


import java.sql.ResultSet;

public interface JdbcResponse {
    default int count() {
        return 0;
    }

    default int[] counts() {
        return new int[0];
    }

    default ResultSet resultSet() {
        return null;
    }

    default boolean success() {
        return true;
    }
}
