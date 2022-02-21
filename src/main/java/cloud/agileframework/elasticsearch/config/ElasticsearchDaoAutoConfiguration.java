package cloud.agileframework.elasticsearch.config;

import cloud.agileframework.common.util.http.HttpUtil;
import cloud.agileframework.elasticsearch.dao.ElasticsearchDao;
import com.alibaba.druid.pool.DruidDataSource;
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

import javax.sql.ConnectionPoolDataSource;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@EnableConfigurationProperties({ElasticsearchProperties.class, ReactiveElasticsearchRestClientProperties.class})
@Configuration
public class ElasticsearchDaoAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RestHighLevelClient.class)
    public RestHighLevelClient restHighLevelClient() throws NoSuchAlgorithmException, KeyManagementException {
        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(InetSocketAddress.createUnresolved("127.0.0.1", 9200))
                .usingSsl(HttpUtil.createIgnoreVerifySSL(SSLConnectionSocketFactory.SSL))
                .withBasicAuth("admin", "admin")    // 如果开启了用户名密码验证，则需要加上
                .build();
        return RestClients.create(clientConfiguration).rest();
    }

    @Bean
    @ConditionalOnMissingBean(ElasticsearchRestTemplate.class)
    public ElasticsearchRestTemplate elasticsearchRestTemplate(@Autowired RestHighLevelClient restHighLevelClient) {
        return new ElasticsearchRestTemplate(restHighLevelClient);
    }

    @Bean
    public static ConnectionPoolDataSource connectionPoolDataSource(ElasticsearchProperties properties) {
        DruidDataSource dataSource = new DruidDataSource();

        List<URI> urls = properties.getUris().stream().map((s) -> s.startsWith("http") ? s : "http://" + s)
                .map(URI::create).collect(Collectors.toList());

        
        dataSource.setUrl(String.format("jdbc:elastic://%s:%s",
                urls.get(0).getHost(),
                urls.get(0).getPort()));
        if("https".equals(urls.get(0).getScheme())){
            Properties prop = new Properties();
            prop.put("useSSL","true");
            prop.put("trustSelfSigned","true");
            dataSource.setConnectProperties(prop);
        }
        
        dataSource.setDriverClassName("cloud.agileframework.elasticsearch.Driver"); //这个可以缺省的，会根据url自动识别
        dataSource.setUsername(properties.getUsername());
        dataSource.setPassword(properties.getPassword());
        //下面都是可选的配置
        dataSource.setValidationQuery("SHOW tables LIKE %");
        dataSource.setInitialSize(10);  //初始连接数，默认0
        dataSource.setMaxActive(30);  //最大连接数，默认8
        dataSource.setMinIdle(10);  //最小闲置数
        dataSource.setMaxWait(2000);  //获取连接的最大等待时间，单位毫秒
        dataSource.setPoolPreparedStatements(true); //缓存PreparedStatement，默认false
        dataSource.setMaxOpenPreparedStatements(20); //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码
        return dataSource;
    }

    @Bean
    public ElasticsearchDao elasticsearchDao(ElasticsearchRestTemplate restTemplate, ConnectionPoolDataSource connectionPoolDataSource) {
        return new ElasticsearchDao(restTemplate, connectionPoolDataSource);
    }
}
