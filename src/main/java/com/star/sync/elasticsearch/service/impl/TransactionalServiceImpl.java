package com.star.sync.elasticsearch.service.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.star.sync.elasticsearch.dao.BaseDao;
import com.star.sync.elasticsearch.model.IndexTypeModel;
import com.star.sync.elasticsearch.model.request.SyncByTableRequest;
import com.star.sync.elasticsearch.service.ElasticsearchService;
import com.star.sync.elasticsearch.service.TransactionalService;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0.0
 * @since 2018-05-21 23:23:00
 */
@Service
public class TransactionalServiceImpl implements TransactionalService {

	@Resource
	private BaseDao baseDao;

	@Resource
	private ElasticsearchService elasticsearchService;

	@Transactional(readOnly = true, rollbackFor = Exception.class)
	@Override
	public void batchInsertElasticsearch(SyncByTableRequest request, String primaryKey, long from, long to,
			IndexTypeModel indexTypeModel) {
		List<Map<String, Object>> dataList = baseDao.selectByPKIntervalLockInShareMode(primaryKey, from, to,
				request.getDatabase(), request.getTable());
		if (dataList == null || dataList.isEmpty()) {
			return;
		}
		dataList = convertDateType(dataList);
		Map<String, Map<String, Object>> dataMap = dataList.parallelStream()
				.collect(Collectors.toMap(strObjMap -> String.valueOf(strObjMap.get(primaryKey)), map -> map));
		elasticsearchService.batchInsertById(indexTypeModel.getIndex(), indexTypeModel.getType(), dataMap);
	}

	private List<Map<String, Object>> convertDateType(List<Map<String, Object>> source) {
		source.parallelStream().forEach(map -> map.forEach((key, value) -> {
			if (value instanceof Timestamp) {
				map.put(key, LocalDateTime.ofInstant(((Timestamp) value).toInstant(), ZoneId.systemDefault()));
			} else if (value instanceof BigDecimal) {
				map.put(key, ((BigDecimal) value).toPlainString());
			} else if (value instanceof java.math.BigInteger) {
				map.put(key, ((BigInteger) value).longValue());
			}
		}));
		List<Map<String, Object>> target = new ArrayList<Map<String, Object>>();
		
		source.forEach(map->{
			Map<String, Object> newMap = new HashMap<>();
			target.add(newMap);
			map.forEach((key,value)->{
				if (key.contains("_")) {
					StringBuffer stringBuffer = new StringBuffer();
					String[] fragments = key.split("_");
					stringBuffer.append(fragments[0]);
					for (int i = 1; i < fragments.length; i++) {
						char uc  = Character.toUpperCase(fragments[i].charAt(0));
						StringBuilder stringBuilder = new StringBuilder(fragments[i]);
						stringBuilder.setCharAt(0, uc);
						stringBuffer.append(stringBuilder.toString());
					}
					String newKey = stringBuffer.toString();
					newMap.put(newKey, value);
				}else {
					newMap.put(key, value);
				}
			});
		});
		return target;
	}
}
