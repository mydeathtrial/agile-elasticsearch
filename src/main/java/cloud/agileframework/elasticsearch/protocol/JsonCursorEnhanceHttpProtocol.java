package cloud.agileframework.elasticsearch.protocol;

import cloud.agileframework.elasticsearch.proxy.JdbcRequest;
import cloud.agileframework.elasticsearch.proxy.JdbcResponse;
import cloud.agileframework.elasticsearch.transport.EnhanceHttpTransport;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.exceptions.ResponseException;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.http.JsonCursorHttpProtocol;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.http.JsonHttpProtocol;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpParam;
import com.amazonaws.opendistro.elasticsearch.sql.jdbc.shadow.org.apache.http.client.methods.CloseableHttpResponse;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class JsonCursorEnhanceHttpProtocol extends JsonHttpProtocol implements EnhanceProtocol {
    private final EnhanceHttpTransport transport;

    public JsonCursorEnhanceHttpProtocol(EnhanceHttpTransport transport) {
        super(transport);
        this.transport = transport;
    }

    public EnhanceHttpTransport getEnhanceTransport() {
        return transport;
    }

    @Override
    public JdbcResponse executeUpdate(JdbcRequest request) throws ResponseException, IOException {
        switch (request.getMethod()) {
            case PUT:
                try (CloseableHttpResponse response = getEnhanceTransport().doPut(
                        request.getUrl(),
                        defaultJsonHeaders,
                        new HttpParam[0],
                        request.getBody(), 0)) {

                    return getJsonHttpResponseHandler()
                            .handleResponse(response, contentStream -> request.getHandler().toResponse(contentStream));

                }
            case POST:
                try (CloseableHttpResponse response = getEnhanceTransport().doPost(
                        request.getUrl(),
                        defaultJsonHeaders,
                        new HttpParam[0],
                        request.getBody(), 0)) {

                    return getJsonHttpResponseHandler()
                            .handleResponse(response, Sets.newHashSet(200,201,204),true, contentStream -> request.getHandler().toResponse(contentStream));
                }
            case DELETE:
                try (CloseableHttpResponse response = getEnhanceTransport().doDelete(
                        request.getUrl(),
                        defaultJsonHeaders,
                        new HttpParam[0], 0)) {

                    return getJsonHttpResponseHandler()
                            .handleResponse(response, contentStream -> request.getHandler().toResponse(contentStream));
                }
            default:
                try (CloseableHttpResponse response = getEnhanceTransport().doGet(
                        request.getUrl(),
                        defaultJsonHeaders,
                        new HttpParam[0], 0)) {

                    return getJsonHttpResponseHandler()
                            .handleResponse(response, contentStream -> request.getHandler().toResponse(contentStream));
                }
        }
    }
}
