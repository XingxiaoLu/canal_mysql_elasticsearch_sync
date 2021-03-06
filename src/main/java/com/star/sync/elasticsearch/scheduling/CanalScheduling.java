package com.star.sync.elasticsearch.scheduling;

import java.util.List;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.star.sync.elasticsearch.client.CanalDestinationsManager;
import com.star.sync.elasticsearch.event.DeleteCanalEvent;
import com.star.sync.elasticsearch.event.InsertCanalEvent;
import com.star.sync.elasticsearch.event.UpdateCanalEvent;
import com.star.sync.elasticsearch.service.SyncService;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-26 22:44:00
 */
@Component
@Slf4j
public class CanalScheduling implements Runnable, ApplicationContextAware {
  private ApplicationContext applicationContext;

  @Autowired
  private SyncService syncService;

  @Autowired
  private CanalDestinationsManager canalDestinationsManager;

  @Scheduled(fixedRate = 100)
  @Override
  public void run() {
    if (syncService.isStartSyncBinlog()) {
      canalDestinationsManager.getCanalConnectors().forEach(canalConnector -> {
        try {
          int batchSize = 1000;
          // Message message = connector.get(batchSize);
          Message message = canalConnector.getWithoutAck(batchSize);
          long batchId = message.getId();
          log.debug("scheduled_batchId=" + batchId);
          try {
            List<Entry> entries = message.getEntries();
            if (batchId != -1 && entries.size() > 0) {
              entries.forEach(entry -> {
                try {
                  RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                  log.info("DdlSchemaName:{}, EventType:{}, Sql:{}", rowChange.getDdlSchemaName(),
                      rowChange.getEventType(), rowChange.getSql());
                } catch (InvalidProtocolBufferException e) {
                  log.error("InvalidProtocolBufferException", e);

                }
                if (entry.getEntryType() == EntryType.ROWDATA) {
                  log.info("开始处理binlog:{}文件的{}；event:{}, database:{}, table:{}",
                      entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                      entry.getHeader().getEventType(), entry.getHeader().getSchemaName(),
                      entry.getHeader().getTableName());
                  publishCanalEvent(entry);
                }
              });
            }
            canalConnector.ack(batchId);
          } catch (Exception e) {
            log.error("发送监听事件失败！batchId回滚,batchId=" + batchId, e);
            canalConnector.rollback(batchId);
          }
        } catch (Exception e) {
          log.error("canal_scheduled异常！", e);
        }
      });
    }
  }

  private void publishCanalEvent(Entry entry) {
    EventType eventType = entry.getHeader().getEventType();
    switch (eventType) {
      case INSERT:
        applicationContext.publishEvent(new InsertCanalEvent(entry));
        break;
      case UPDATE:
        applicationContext.publishEvent(new UpdateCanalEvent(entry));
        break;
      case DELETE:
        applicationContext.publishEvent(new DeleteCanalEvent(entry));
        break;
      default:
        break;
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }
}
