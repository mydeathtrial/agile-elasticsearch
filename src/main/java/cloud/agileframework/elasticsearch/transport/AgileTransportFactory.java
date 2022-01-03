package cloud.agileframework.elasticsearch.transport;

import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportFactory;

public class AgileTransportFactory implements TransportFactory<AgileHttpTransport> {
    public static AgileTransportFactory INSTANCE = new AgileTransportFactory();

    @Override
    public AgileHttpTransport getTransport(ConnectionConfig config, Logger log, String userAgent) throws TransportException {
        return new AgileHttpTransport(config, log, userAgent);
    }
}
