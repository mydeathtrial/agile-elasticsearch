package cloud.agileframework.elasticsearch.transport;

import com.amazon.opendistroforelasticsearch.jdbc.auth.AuthenticationType;
import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.logging.Logger;
import com.amazon.opendistroforelasticsearch.jdbc.logging.LoggingSource;
import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.ApacheHttpClientConnectionFactory;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpParam;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.JclLoggerAdapter;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.Header;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.HttpResponse;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.auth.AuthScope;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.auth.UsernamePasswordCredentials;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.CredentialsProvider;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.HttpClient;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.config.RequestConfig;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.CloseableHttpResponse;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpDelete;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpGet;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpPost;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.HttpPut;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.utils.URIBuilder;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.config.Registry;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.config.RegistryBuilder;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.config.SocketConfig;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.conn.socket.ConnectionSocketFactory;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.conn.socket.PlainConnectionSocketFactory;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.conn.ssl.NoopHostnameVerifier;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.entity.ContentType;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.entity.StringEntity;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.client.BasicCredentialsProvider;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.client.CloseableHttpClient;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.client.HttpClientBuilder;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.client.HttpClients;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.ssl.SSLContextBuilder;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.ssl.SSLContexts;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class AgileHttpTransport implements EnhanceHttpTransport, HttpTransport, LoggingSource {

    private final String scheme;
    private final String host;
    private final int port;
    private final String path;
    private final CloseableHttpClient httpClient;
    private int readTimeout;
    private RequestConfig requestConfig;

    public AgileHttpTransport(ConnectionConfig connectionConfig, Logger log, String userAgent) throws TransportException {
        this.host = connectionConfig.getHost();
        this.port = connectionConfig.getPort();
        this.scheme = connectionConfig.isUseSSL() ? "https" : "http";
        this.path = connectionConfig.getPath();

        this.requestConfig = RequestConfig.custom()
                .setSocketTimeout(this.readTimeout)
                .build();

        ConnectionSocketFactory sslConnectionSocketFactory;

        try {
            sslConnectionSocketFactory = getSslConnectionSocketFactory(connectionConfig);
        } catch (Exception e) {
            throw new TransportException("Exception building SSL/TLS socket factory " + e, e);
        }

        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslConnectionSocketFactory)
                .build();

        ApacheHttpClientConnectionFactory connectionFactory =
                new ApacheHttpClientConnectionFactory(new JclLoggerAdapter(log, getSource()));

        PoolingHttpClientConnectionManager poolingConnMgr = new PoolingHttpClientConnectionManager(socketFactoryRegistry, connectionFactory);
        poolingConnMgr.setMaxTotal(1000);
        poolingConnMgr.setDefaultMaxPerRoute(1000);

        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setConnectionManager(
                        poolingConnMgr)
                .setDefaultSocketConfig(SocketConfig.custom()
                        .setSoKeepAlive(true)
                        .setSoTimeout(this.readTimeout)
                        .build())
                .setDefaultRequestConfig(requestConfig)
                .setUserAgent(userAgent);

        // request compression
        if (!connectionConfig.requestCompression())
            httpClientBuilder.disableContentCompression();

        // setup authentication
        if (connectionConfig.getAuthenticationType() == AuthenticationType.BASIC) {
            CredentialsProvider basicCredsProvider = new BasicCredentialsProvider();
            basicCredsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(connectionConfig.getUser(), connectionConfig.getPassword()));
            httpClientBuilder.setDefaultCredentialsProvider(basicCredsProvider);
        }
        httpClientBuilder.setConnectionManagerShared(true);
        this.httpClient = httpClientBuilder.build();
    }

    private HttpClient getHttpClient() {
        return httpClient;
    }

    private ConnectionSocketFactory getSslConnectionSocketFactory(ConnectionConfig connectionConfig)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException,
            UnrecoverableKeyException, KeyManagementException {
        TrustStrategy trustStrategy = connectionConfig.trustSelfSigned() ? new TrustSelfSignedStrategy() : null;

        SSLContextBuilder builder = SSLContexts.custom();

        if (connectionConfig.getKeyStoreLocation() != null || connectionConfig.getTrustStoreLocation() != null) {
            // trust material
            if (connectionConfig.getTrustStoreLocation() != null) {
                String trustStorePassword = connectionConfig.getTrustStorePassword();
                char[] password = trustStorePassword == null ? "".toCharArray() : trustStorePassword.toCharArray();

                builder.loadTrustMaterial(
                        new File(connectionConfig.getTrustStoreLocation()),
                        password, trustStrategy);
            }

            // key material
            if (connectionConfig.getKeyStoreLocation() != null) {
                String keyStorePassword = connectionConfig.getKeyStorePassword();
                char[] password = keyStorePassword == null ? "".toCharArray() : keyStorePassword.toCharArray();

                // TODO - can add alias selection strategy
                // TODO - can add support for a separate property for key password
                builder.loadKeyMaterial(new File(connectionConfig.getKeyStoreLocation()), password, password).build();
            }

        } else {

            builder.loadTrustMaterial(null, trustStrategy);
        }

        HostnameVerifier hostnameVerifier = connectionConfig.hostnameVerification() ?
                SSLConnectionSocketFactory.getDefaultHostnameVerifier() : new NoopHostnameVerifier();

        SSLContext sslContext = builder.build();
        return new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
    }

    @Override
    public HttpResponse doPut(String path, Header[] headers, HttpParam[] params, String body, int timeout) throws TransportException {
        return doPut(buildRequestURI(path, params), headers, body, timeout);
    }

    private HttpResponse doPut(URI uri, Header[] headers, String body, int timeout) throws TransportException {
        try {
            setReadTimeout(timeout);
            HttpPut request = new HttpPut(uri);
            request.setHeaders(headers);
            request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            request.setConfig(requestConfig);
            return getHttpClient().execute(request);
        } catch (IOException e) {
            throw new TransportException(e);
        }
    }

    @Override
    public CloseableHttpResponse doGet(String path, Header[] headers, HttpParam[] params, int timeout) throws TransportException {
        return doGet(buildRequestURI(path, params), headers, readTimeout);
    }

    @Override
    public CloseableHttpResponse doPost(String path, Header[] headers, HttpParam[] params, String body, int timeout) throws TransportException {
        return doPost(buildRequestURI(path, params), headers, body, readTimeout);
    }

    private CloseableHttpResponse doGet(URI uri, Header[] headers, int readTimeout) throws TransportException {
        try {
            this.setReadTimeout(readTimeout);
            HttpGet request = new HttpGet(uri);
            request.setHeaders(headers);
            request.setConfig(requestConfig);
            return (CloseableHttpResponse) getHttpClient().execute(request);
        } catch (IOException var5) {
            throw new TransportException(var5);
        }
    }

    private CloseableHttpResponse doPost(URI uri, Header[] headers, String body, int readTimeout) throws TransportException {
        try {
            this.setReadTimeout(readTimeout);
            HttpPost request = new HttpPost(uri);
            request.setHeaders(headers);
            request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            request.setConfig(requestConfig);
            return (CloseableHttpResponse) getHttpClient().execute(request);
        } catch (IOException var6) {
            throw new TransportException(var6);
        }
    }

    @Override
    public HttpResponse doDelete(String path, Header[] headers, HttpParam[] params, int timeout) throws TransportException {
        return doDelete(buildRequestURI(path, params), headers, timeout);
    }

    private HttpResponse doDelete(URI uri, Header[] headers, int timeout) throws TransportException {
        try {
            setReadTimeout(timeout);
            HttpDelete request = new HttpDelete(uri);
            request.setHeaders(headers);
            request.setConfig(requestConfig);
            return getHttpClient().execute(request);
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

    @Override
    public void close() throws TransportException {
        try {
            this.httpClient.close();
        } catch (IOException e) {
            throw new TransportException(e);
        }
    }

    @Override
    public void setReadTimeout(int readTimeout) {
        if (readTimeout != this.readTimeout) {
            this.readTimeout = readTimeout;
            this.requestConfig = RequestConfig.custom()
                    .setSocketTimeout(this.readTimeout)
                    .build();
        }
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
