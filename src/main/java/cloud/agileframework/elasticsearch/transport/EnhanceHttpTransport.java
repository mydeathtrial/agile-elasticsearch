package cloud.agileframework.elasticsearch.transport;

import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpParam;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.Header;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.CloseableHttpResponse;

public interface EnhanceHttpTransport extends HttpTransport {
    CloseableHttpResponse doPut(String path, Header[] headers, HttpParam[] params, String body, int timeout)
            throws TransportException;

    CloseableHttpResponse doDelete(String path, Header[] headers, HttpParam[] params, int timeout)
            throws TransportException;
}
