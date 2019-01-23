package com.star.sync.elasticsearch.client;

import java.net.InetAddress;
import java.util.Base64;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.star.sync.elasticsearch.config.ElasticsearchProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-25 17:32:00
 */
@Component
@EnableConfigurationProperties(ElasticsearchProperties.class)
@Slf4j
public class ElasticsearchClient implements DisposableBean {
  private TransportClient transportClient;
  @Autowired
  private ElasticsearchProperties elasticsearchProperties;

  @Bean
  public TransportClient getTransportClient() throws Exception {
    Settings settings =
        Settings.builder().put("cluster.name", elasticsearchProperties.getClustername())
            .put("client.transport.sniff", true)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
            .put("elasticsearch.username", elasticsearchProperties.getUsername())
            .put("elasticsearch.password", elasticsearchProperties.getPassword()).build();
    transportClient =
        new PreBuiltTransportClient(settings, SearchGuardPlugin.class).addTransportAddress(
            new InetSocketTransportAddress(InetAddress.getByName(elasticsearchProperties.getHost()),
                Integer.valueOf(elasticsearchProperties.getPort())));
    transportClient.threadPool().getThreadContext().putHeader("Authorization",
        "Basic " + Base64.getEncoder().encode(
            (elasticsearchProperties.getUsername() + ":" + elasticsearchProperties.getPassword())
                .getBytes()));
    log.info("elasticsearch transportClient 连接成功");
    return transportClient;
  }

  @Override
  public void destroy() throws Exception {
    if (transportClient != null) {
      transportClient.close();
    }
  }
}
