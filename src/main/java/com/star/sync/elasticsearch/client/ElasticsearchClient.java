package com.star.sync.elasticsearch.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Base64;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
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
    log.info("初始化 elasticsearch client");
    String tmp = System.getProperty("java.io.tmpdir");
    log.info("tmp：{}", tmp);
    String pemcertFile = tmp + File.separator + "esnode.pem";
    String pemkeyFile = tmp + File.separator + "esnode-key.pem";
    String pemTrustedcasFile = tmp + File.separator + "root-ca.pem";
    try {
      InputStream certInput =
          new ClassPathResource(elasticsearchProperties.getPemcertFilePath()).getInputStream();
      InputStream keyInput =
          new ClassPathResource(elasticsearchProperties.getPemkeyFilePath()).getInputStream();
      InputStream trustedInput =
          new ClassPathResource(elasticsearchProperties.getPemTrustedcasFilePath())
              .getInputStream();
      FileOutputStream certStream = new FileOutputStream(pemcertFile);
      FileOutputStream keyStream = new FileOutputStream(pemkeyFile);
      FileOutputStream trustedStream = new FileOutputStream(pemTrustedcasFile);
      IOUtils.copy(certInput, certStream);
      IOUtils.copy(keyInput, keyStream);
      IOUtils.copy(trustedInput, trustedStream);
    } catch (IOException e) {
      log.error("写入jks文件异常：{}", e);
    }
    Settings settings = Settings.builder()
        .put("cluster.name", elasticsearchProperties.getClustername())
        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, pemcertFile)
        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, pemkeyFile)
        .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, pemTrustedcasFile)
        .put("searchguard.ssl.transport.enforce_hostname_verification", false).build();
    transportClient =
        new PreBuiltTransportClient(settings, SearchGuardPlugin.class).addTransportAddress(
            new TransportAddress(InetAddress.getByName(elasticsearchProperties.getHost()),
                Integer.valueOf(elasticsearchProperties.getPort())));
//    transportClient.threadPool().getThreadContext().putHeader("Authorization",
//        "Basic " + Base64.getEncoder().encode(
//            (elasticsearchProperties.getUsername() + ":" + elasticsearchProperties.getPassword())
//                .getBytes()));
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
