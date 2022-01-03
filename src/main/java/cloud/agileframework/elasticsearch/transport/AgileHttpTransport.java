package cloud.agileframework.elasticsearch.transport;

import cloud.agileframework.common.util.clazz.ClassUtil;
import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.ApacheHttpTransport;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpParam;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.Header;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.config.RequestConfig;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.CloseableHttpResponse;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpDelete;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpPut;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.utils.URIBuilder;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.entity.ContentType;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.entity.StringEntity;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

public class AgileHttpTransport extends ApacheHttpTransport implements EnhanceHttpTransport {

    private final String scheme;
    private final String host;
    private final int port;
    private final String path;
    private int readTimeout;
    private CloseableHttpClient httpClient;

    public AgileHttpTransport(ConnectionConfig connectionConfig, Logger log, String userAgent) throws TransportException {
        super(connectionConfig, log, userAgent);
        this.host = connectionConfig.getHost();
        this.port = connectionConfig.getPort();
        this.scheme = connectionConfig.isUseSSL() ? "https" : "http";
        this.path = connectionConfig.getPath();
        Field httpClientField = ClassUtil.getField(ApacheHttpTransport.class, "httpClient");
        Field readTimeoutField = ClassUtil.getField(ApacheHttpTransport.class, "readTimeout");

        try {
            httpClient = (CloseableHttpClient) httpClientField.get(this);
            readTimeout = (int) readTimeoutField.get(this);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public CloseableHttpResponse doPut(String path, Header[] headers, HttpParam[] params, String body, int timeout) throws TransportException {
        return doPut(buildRequestURI(path, params), headers, body, timeout);
    }

    private CloseableHttpResponse doPut(URI uri, Header[] headers, String body, int timeout) throws TransportException {
        try {
            setReadTimeout(readTimeout);
            HttpPut request = new HttpPut(uri);
            request.setHeaders(headers);
            request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            request.setConfig(RequestConfig.custom().setSocketTimeout(readTimeout).build());
            return httpClient.execute(request);
        } catch (IOException e) {
            throw new TransportException(e);
        }
    }

    @Override
    public CloseableHttpResponse doDelete(String path, Header[] headers, HttpParam[] params, int timeout) throws TransportException {
        return doDelete(buildRequestURI(path, params), headers, timeout);
    }

    private CloseableHttpResponse doDelete(URI uri, Header[] headers, int timeout) throws TransportException {
        try {
            setReadTimeout(readTimeout);
            HttpDelete request = new HttpDelete(uri);
            request.setHeaders(headers);
            request.setConfig(RequestConfig.custom().setSocketTimeout(readTimeout).build());
            return httpClient.execute(request);
        } catch (IOException e) {
            throw new TransportException(e);
        }
    }

    private URIBuilder getUriBuilder(String path) {
        return new URIBuilder()
                .setScheme(this.scheme)
                .setHost(this.host)
                .setPort(this.port)
                .setPath(this.path + path);
    }

    private URI buildRequestURI(String path, HttpParam... params) throws TransportException {
        try {
            URIBuilder uriBuilder = getUriBuilder(path);

            if (params != null) {
                for (HttpParam param : params)
                    uriBuilder.setParameter(param.getName(), param.getValue());
            }
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new TransportException(e);
        }
    }
}
