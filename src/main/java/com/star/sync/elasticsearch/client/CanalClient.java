package com.star.sync.elasticsearch.client;

import java.net.InetSocketAddress;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.google.common.collect.Lists;
import com.star.sync.elasticsearch.config.CanalProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-25 17:26:00
 */
@Component
@EnableConfigurationProperties(CanalProperties.class)
@Slf4j
public class CanalClient implements DisposableBean {
  private CanalDestinationsManager canalDestinationsManager;

  @Autowired
  private CanalProperties canalProperties;

  @Bean
  public CanalDestinationsManager getCanalDestinationsManager() {
    canalDestinationsManager = new CanalDestinationsManager();
    String[] destinations = canalProperties.getDestinations().split(",");
    for (String destination : destinations) {
      CanalConnector canalConnector = CanalConnectors.newClusterConnector(
          Lists.newArrayList(new InetSocketAddress(canalProperties.getHost(),
              Integer.valueOf(canalProperties.getPort()))),
          destination, canalProperties.getUsername(), canalProperties.getPassword());
      canalConnector.connect();
      // 指定filter，格式 {database}.{table}，这里不做过滤，过滤操作留给用户
      canalConnector.subscribe();
      // 回滚寻找上次中断的位置
      canalConnector.rollback();
      canalDestinationsManager.getCanalConnectors().add(canalConnector);
    }
    log.info("canal客户端启动成功");
    return canalDestinationsManager;
  }

  @Override
  public void destroy() throws Exception {
    canalDestinationsManager.getCanalConnectors().forEach(canalConnector -> {
      if (canalConnector != null) {
        canalConnector.disconnect();
      }
    });
  }
}
