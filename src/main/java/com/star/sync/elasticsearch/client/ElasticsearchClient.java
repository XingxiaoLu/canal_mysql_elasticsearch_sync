package com.star.sync.elasticsearch.client;

import java.net.InetAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-25 17:32:00
 */
@Component
@Slf4j
public class ElasticsearchClient implements DisposableBean {
  private TransportClient transportClient;

  @Value("${elasticsearch.cluster.name}")
  private String clusterName;
  @Value("${elasticsearch.host}")
  private String host;
  @Value("${elasticsearch.port}")
  private String port;

  @Bean
  public TransportClient getTransportClient() throws Exception {
    Settings settings = Settings.builder().put("cluster.name", clusterName)
        .put("client.transport.sniff", true).build();
    transportClient = new PreBuiltTransportClient(settings).addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName(host), Integer.valueOf(port)));
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
