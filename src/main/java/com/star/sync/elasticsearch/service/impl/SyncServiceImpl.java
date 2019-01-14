package com.star.sync.elasticsearch.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.google.common.collect.Lists;
import com.star.sync.elasticsearch.dao.BaseDao;
import com.star.sync.elasticsearch.model.DatabaseTableModel;
import com.star.sync.elasticsearch.model.IndexTypeModel;
import com.star.sync.elasticsearch.model.request.SyncByTableRequest;
import com.star.sync.elasticsearch.service.MappingService;
import com.star.sync.elasticsearch.service.SyncService;
import com.star.sync.elasticsearch.service.TransactionalService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-31 17:48:00
 */
@Service
@Slf4j
public class SyncServiceImpl implements SyncService, InitializingBean, DisposableBean {
  /**
   * 使用线程池控制并发数量
   */
  private ExecutorService cachedThreadPool;

  private boolean startSyncBinlog = false;

  @Autowired
  private BaseDao baseDao;

  @Autowired
  private MappingService mappingService;

  @Autowired
  private TransactionalService transactionalService;

  public final static long STEP_LENGTH = 5 * 100 * 10000;

  @Override
  public boolean syncByTable(SyncByTableRequest request) {
    IndexTypeModel indexTypeModel = mappingService
        .getIndexType(new DatabaseTableModel(request.getDatabase(), request.getTable()));
    String primaryKey = Optional.ofNullable(mappingService.getTablePrimaryKeyMap()
        .get(request.getDatabase() + "." + request.getTable())).orElse("id");
    if (indexTypeModel == null) {
      throw new IllegalArgumentException(
          String.format("配置文件中缺失database=%s和table=%s所对应的index和type的映射配置", request.getDatabase(),
              request.getTable()));
    }
    if (baseDao.selectMinPK(primaryKey, request.getDatabase(), request.getTable()) == null) {
      return true;
    }

    long minPK = Optional.ofNullable(request.getFrom())
        .orElse(baseDao.selectMinPK(primaryKey, request.getDatabase(), request.getTable()));
    long maxPK = Optional.ofNullable(request.getTo())
        .orElse(baseDao.selectMaxPK(primaryKey, request.getDatabase(), request.getTable()));
    minPK = Optional.ofNullable(minPK).orElse(0l);
    maxPK = Optional.ofNullable(maxPK).orElse(0l);
    long number = maxPK - minPK;

    List<SplitResult> splitResults = split(number, minPK);
    for (SplitResult splitResult : splitResults) {
      commitJob(request, indexTypeModel, primaryKey, splitResult.getFrom(), splitResult.getTo());
    }
    return true;
  }

  /**
   * @param request
   * @param indexTypeModel
   * @param primaryKey
   * @param from
   * @param to
   */
  private void commitJob(SyncByTableRequest request, IndexTypeModel indexTypeModel,
      String primaryKey, long from, long to) {
    cachedThreadPool.submit(() -> {
      try {
        for (long i = from; i < to; i += request.getStepSize()) {
          transactionalService.batchInsertElasticsearch(request, primaryKey, i,
              i + request.getStepSize(), indexTypeModel);
          log.info("表 {} from {} to {} : 当前同步pk={}, 进度={}%", request.getTable(), from, to, i,
              (i - from) * 100L / (to - from));
        }
        log.info("表 {} from {} to {} 同步完成", request.getTable(), from, to);
      } catch (Exception e) {
        log.error("数据库 {} 数据表 {} 操作异常", request.getDatabase(), request.getTable());
        log.error("批量转换并插入Elasticsearch异常", e);
      }
    });
  }

  private List<SplitResult> split(long number, long minPk) {
    List<SplitResult> result = Lists.newArrayList();
    long time = number / STEP_LENGTH + 1;
    for (int i = 0; i < time; i++) {
      SplitResult r = new SplitResult();
      r.setFrom(minPk + i * STEP_LENGTH);
      r.setTo(minPk + Math.min((i + 1) * STEP_LENGTH, number));
      result.add(r);
    }
    return result;
  }

  @Data
  public static class SplitResult {
    private long from;
    private long to;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    cachedThreadPool = new ThreadPoolExecutor(100, 500, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(), (ThreadFactory) Thread::new);
  }

  @Override
  public void destroy() throws Exception {
    if (cachedThreadPool != null) {
      cachedThreadPool.shutdown();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.star.sync.elasticsearch.service.SyncService#syncAll()
   */
  @Override
  public boolean syncAll() {
    Map<String, String> dbEsMapping = mappingService.getDbEsMapping();
    dbEsMapping.forEach((key, value) -> {
      String[] fragments = key.split("\\.");
      if (fragments.length == 2) {
        String database = fragments[0];
        String table = fragments[1];

        SyncByTableRequest request = new SyncByTableRequest();
        request.setDatabase(database);
        request.setTable(table);

        log.info("开始同步-> 数据库: {}, 数据表: {}", database, table);
        syncByTable(request);
      }
    });

    log.info("全量同步完成");
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.star.sync.elasticsearch.service.SyncService#startSyncBinlog()
   */
  @Override
  public boolean startSyncBinlog() {
    startSyncBinlog = true;
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.star.sync.elasticsearch.service.SyncService#isStartSyncBinlog()
   */
  @Override
  public boolean isStartSyncBinlog() {
    return startSyncBinlog;
  }
}
