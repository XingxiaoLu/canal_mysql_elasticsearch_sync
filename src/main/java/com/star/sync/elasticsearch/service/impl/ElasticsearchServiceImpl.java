package com.star.sync.elasticsearch.service.impl;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Resource;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.stereotype.Service;
import com.star.sync.elasticsearch.service.ElasticsearchService;
import com.star.sync.elasticsearch.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-27 12:38:00
 */
@Service
@Slf4j
public class ElasticsearchServiceImpl implements ElasticsearchService {

  @Resource
  private TransportClient transportClient;

  @Override
  public void insertById(String index, String type, String id, Map<String, Object> dataMap) {
    transportClient.prepareIndex(index, type, id).setSource(dataMap).get();
  }

  @Override
  public void batchInsertById(String index, String type,
      Map<String, Map<String, Object>> idDataMap) {
    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

    idDataMap.forEach((id, dataMap) -> bulkRequestBuilder
        .add(transportClient.prepareIndex(index, type, id).setSource(dataMap)));
    try {
      BulkResponse bulkResponse = bulkRequestBuilder.execute().get();
      if (bulkResponse.hasFailures()) {
        log.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data="
            + JsonUtil.toJson(idDataMap) + ", cause:" + bulkResponse.buildFailureMessage());
      }
    } catch (Exception e) {
      log.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data="
          + JsonUtil.toJson(idDataMap), e);
    }
  }

  @Override
  public void update(String index, String type, String id, Map<String, Object> dataMap) {
    this.insertById(index, type, id, dataMap);
  }

  @Override
  public void deleteById(String index, String type, String id) {
    transportClient.prepareDelete(index, type, id).get();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.star.sync.elasticsearch.service.ElasticsearchService#batchUpdateById(java.lang.String,
   * java.lang.String, java.util.Map)
   */
  @Override
  public void batchUpdateById(String index, String type,
      Map<String, Map<String, Object>> idDataMap) {
    this.batchInsertById(index, type, idDataMap);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.star.sync.elasticsearch.service.ElasticsearchService#batchDeleteById(java.lang.String,
   * java.lang.String, java.util.Collection)
   */
  @Override
  public void batchDeleteById(String index, String type, Collection<String> ids) {
    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
    ids.forEach(id -> bulkRequestBuilder.add(transportClient.prepareIndex(index, type, id)));

    try {
      BulkResponse bulkResponse = bulkRequestBuilder.execute().get();
      if (bulkResponse.hasFailures()) {
        log.error("elasticsearch批量删除错误, index=" + index + ", type=" + type + ", cause:"
            + bulkResponse.buildFailureMessage());
      }
    } catch (Exception e) {
      log.error("elasticsearch批量删除错误, index=" + index + ", type=" + type + ".", e);
    }
  }
}
