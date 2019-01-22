/**
 * 
 */
package com.star.sync.elasticsearch.client;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import com.alibaba.otter.canal.client.CanalConnector;
import lombok.Getter;

/**
 * @author luxingxiao
 *
 */
public class CanalDestinationsManager {
  @Getter
  private Set<CanalConnector> canalConnectors = new CopyOnWriteArraySet<>();
}
