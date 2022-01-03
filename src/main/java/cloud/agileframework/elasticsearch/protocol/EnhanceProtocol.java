package cloud.agileframework.elasticsearch.protocol;

import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.Protocol;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.exceptions.ResponseException;

import java.io.IOException;

public interface EnhanceProtocol extends Protocol {
    JdbcResponse executeUpdate(JdbcRequest request) throws ResponseException, IOException;
}
