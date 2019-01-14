package com.star.sync.elasticsearch.listener;

import java.util.List;
import java.util.Optional;
import javax.annotation.Resource;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.star.sync.elasticsearch.event.DeleteCanalEvent;
import com.star.sync.elasticsearch.scheduling.DeleteEsDataScheduling;
import com.star.sync.elasticsearch.service.MappingService;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-26 22:33:00
 */
@Component
@Slf4j
public class DeleteCanalListener extends AbstractCanalListener<DeleteCanalEvent> {
  @Resource
  private MappingService mappingService;

  @Autowired
  private DeleteEsDataScheduling deleteScheduling;

  @Override
  protected void doSync(String database, String table, String index, String type, RowData rowData) {
    List<Column> columns = rowData.getBeforeColumnsList();
    String primaryKey =
        Optional.ofNullable(mappingService.getTablePrimaryKeyMap().get(database + "." + table))
            .orElse("id");
    Column idColumn =
        columns.stream().filter(column -> column.getIsKey() && primaryKey.equals(column.getName()))
            .findFirst().orElse(null);
    if (idColumn == null || StringUtils.isBlank(idColumn.getValue())) {
      log.warn("insert_column_find_null_warn insert从column中找不到主键,database=" + database + ",table="
          + table);
      return;
    }
    log.debug("insert_column_id_info insert主键id,database=" + database + ",table=" + table + ",id="
        + idColumn.getValue());
    // elasticsearchService.deleteById(index, type, idColumn.getValue());
    deleteScheduling.addDeleteData(index, type, idColumn.getValue());
    log.debug("insert_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",id="
        + idColumn.getValue());
  }
}
