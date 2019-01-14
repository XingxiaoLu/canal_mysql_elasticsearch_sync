/**
 * 
 */
package com.star.sync.elasticsearch.scheduling;

import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.star.sync.elasticsearch.service.ElasticsearchService;

/**
 * @author luxingxiao
 *
 */
@Component
public class DeleteEsDataScheduling {
  @Autowired
  private ElasticsearchService elasticsearchService;

  private Map<String, Set<String>> shouldBeDeleted = Maps.newConcurrentMap();

  private int num;

  public void addDeleteData(String index, String type, String id) {
    Set<String> ids = shouldBeDeleted.get(index + "." + type);
    if (ids == null) {
      ids = Sets.newConcurrentHashSet();
      shouldBeDeleted.put(index + "." + type, ids);
    }
    ids.add(id);
  }

  @Scheduled(fixedRate = 5)
  public void batchDelete() {
    num++;
    if (num == 1000) {
      doDelete();
      num = 0;
    } else if (idNumber()) {
      doDelete();
    }
  }

  private boolean idNumber() {
    return shouldBeDeleted.values().stream().anyMatch(value -> value.size() == 2000);
  }

  private void doDelete() {
    shouldBeDeleted.forEach((key, value) -> {
      String[] keys = key.split("\\.");
      String index = keys[0];
      String type = keys[1];
      elasticsearchService.batchDeleteById(index, type, value);
      value.clear();
    });
  }
}
