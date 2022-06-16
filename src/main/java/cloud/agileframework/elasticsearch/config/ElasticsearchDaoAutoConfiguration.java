package cloud.agileframework.elasticsearch.config;

import cloud.agileframework.common.util.http.HttpUtil;
import cloud.agileframework.elasticsearch.dao.ElasticsearchDao;
import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.elasticsearch.ReactiveElasticsearchRestClientProperties;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.net.InetSocketAddress;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@EnableConfigurationProperties({ElasticsearchProperties.class, ReactiveElasticsearchRestClientProperties.class})
@Configuration
public class ElasticsearchDaoAutoConfiguration {

    public static DruidDataSource connectionPoolDataSource(ElasticsearchProperties properties) {
        DruidDataSource dataSource = new DruidDataSource();

        dataSource.setUrl(String.join(",", properties.getUris()));
        dataSource.setDriverClassName("cloud.agileframework.elasticsearch.Driver"); //这个可以缺省的，会根据url自动识别
        dataSource.setUsername(properties.getUsername());
        dataSource.setPassword(properties.getPassword());
        //下面都是可选的配置
        dataSource.setInitialSize(10);  //初始连接数，默认0
        dataSource.setMaxActive(30);  //最大连接数，默认8
        dataSource.setMinIdle(10);  //最小闲置数
        dataSource.setMaxWait(2000);  //获取连接的最大等待时间，单位毫秒
        dataSource.setPoolPreparedStatements(false);
        dataSource.setMaxOpenPreparedStatements(20); //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码
        return dataSource;
    }

    @Bean
    @ConditionalOnMissingBean(RestHighLevelClient.class)
    public RestHighLevelClient restHighLevelClient(ElasticsearchProperties properties) throws NoSuchAlgorithmException, KeyManagementException {

        boolean ssl = false;
        List<InetSocketAddress> list = Lists.newArrayList();
        for (String uriString : properties.getUris()) {
            URI uri = URI.create(uriString);
            list.add(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort()));
            if ("https".equals(uri.getScheme())) {
                ssl = true;
            }
        }

        ClientConfiguration.MaybeSecureClientConfigurationBuilder builder = ClientConfiguration.builder().connectedTo(list.toArray(new InetSocketAddress[]{}));
        if (ssl) {
            builder.usingSsl(HttpUtil.createIgnoreVerifySSL(SSLConnectionSocketFactory.SSL), NoopHostnameVerifier.INSTANCE)
                    .withBasicAuth(properties.getUsername(), properties.getPassword());
        }
        return RestClients.create(builder.build()).rest();
    }

    @Bean
    @ConditionalOnMissingBean(ElasticsearchRestTemplate.class)
    public ElasticsearchRestTemplate elasticsearchRestTemplate(@Autowired RestHighLevelClient restHighLevelClient) {
        return new ElasticsearchRestTemplate(restHighLevelClient);
    }

    @Bean
    public ElasticsearchDao elasticsearchDao(ElasticsearchRestTemplate restTemplate, ElasticsearchProperties properties) {
        return new ElasticsearchDao(restTemplate, connectionPoolDataSource(properties));
    }
}
