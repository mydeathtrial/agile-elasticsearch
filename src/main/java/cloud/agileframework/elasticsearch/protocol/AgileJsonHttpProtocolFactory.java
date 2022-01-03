package cloud.agileframework.elasticsearch.protocol;

import cloud.agileframework.elasticsearch.transport.EnhanceHttpTransport;
import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.ProtocolFactory;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.http.JsonCursorHttpProtocol;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.http.JsonHttpProtocol;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;

public class AgileJsonHttpProtocolFactory implements ProtocolFactory<JsonHttpProtocol, HttpTransport> {
    public static AgileJsonHttpProtocolFactory INSTANCE = new AgileJsonHttpProtocolFactory();

    @Override
    public JsonHttpProtocol getProtocol(ConnectionConfig config, HttpTransport transport) {
        return new JsonCursorEnhanceHttpProtocol((EnhanceHttpTransport) transport);
    }
}
