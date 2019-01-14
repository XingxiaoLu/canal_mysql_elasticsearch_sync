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
public class InsertEsDataScheduling {
  @Autowired
  private ElasticsearchService elasticsearchService;

  private Map<String, Map<String, Map<String, Object>>> shouldBeInserted = Maps.newConcurrentMap();

  private int num;

  public void addInsertData(String index, String type, String id, Map<String, Object> dataMap) {
    Map<String, Map<String, Object>> dataMaps = shouldBeInserted.get(index + "." + type);
    if (dataMaps == null) {
      dataMaps = Maps.newConcurrentMap();
      shouldBeInserted.put(index + "." + type, dataMaps);
    }
    dataMaps.put(id, dataMap);
  }

  @Scheduled(fixedRate = 5)
  public void batchInsert() {
    num++;
    if (num == 1000) {
      doInsert();
      num = 0;
    } else if (idNumber()) {
      doInsert();
    }
  }

  private boolean idNumber() {
    return shouldBeInserted.values().stream().anyMatch(value -> value.size() == 2000);
  }

  private void doInsert() {
    shouldBeInserted.forEach((key, value) -> {
      String[] keys = key.split("\\.");
      String index = keys[0];
      String type = keys[1];
      elasticsearchService.batchInsertById(index, type, value);
      value.clear();
    });
  }
}
