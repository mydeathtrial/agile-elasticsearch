package cloud.agileframework.elasticsearch.transport;

import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpParam;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.Header;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.HttpResponse;

public interface EnhanceHttpTransport extends HttpTransport {
    HttpResponse doPut(String path, Header[] headers, HttpParam[] params, String body, int timeout)
            throws TransportException;

    HttpResponse doDelete(String path, Header[] headers, HttpParam[] params, int timeout)
            throws TransportException;
}
