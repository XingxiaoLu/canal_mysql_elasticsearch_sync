/**
 * 
 */
package com.star.sync.elasticsearch.scheduling;

import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.google.common.collect.Maps;
import com.star.sync.elasticsearch.service.ElasticsearchService;

/**
 * @author luxingxiao
 *
 */
@Component
public class UpdateEsDataScheduling {
  @Autowired
  private ElasticsearchService elasticsearchService;

  private Map<String, Map<String, Map<String, Object>>> shouldBeUpdated = Maps.newConcurrentMap();

  private int num;

  public void addUpdateData(String index, String type, String id, Map<String, Object> dataMap) {
    Map<String, Map<String, Object>> dataMaps = shouldBeUpdated.get(index + "." + type);
    if (dataMaps == null) {
      dataMaps = Maps.newConcurrentMap();
      shouldBeUpdated.put(index + "." + type, dataMaps);
    }
    dataMaps.put(id, dataMap);
  }

  @Scheduled(fixedRate = 5)
  public void batchUpdate() {
    num++;
    if (num == 1000) {
      doUpdate();
      num = 0;
    } else if (idNumber()) {
      doUpdate();
    }
  }

  private boolean idNumber() {
    return shouldBeUpdated.values().stream().anyMatch(value -> value.size() == 2000);
  }

  private void doUpdate() {
    shouldBeUpdated.forEach((key, value) -> {
      String[] keys = key.split("\\.");
      String index = keys[0];
      String type = keys[1];
      elasticsearchService.batchUpdateById(index, type, value);
      value.clear();
    });
  }
}
