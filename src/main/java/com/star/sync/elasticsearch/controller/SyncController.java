package com.star.sync.elasticsearch.controller;

import javax.annotation.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import com.star.sync.elasticsearch.model.request.SyncByTableRequest;
import com.star.sync.elasticsearch.model.response.Response;
import com.star.sync.elasticsearch.service.SyncService;
import com.star.sync.elasticsearch.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;


/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-29 19:32:00
 */
@Controller
@RequestMapping("/sync")
@Slf4j
public class SyncController {

  @Resource
  private SyncService syncService;

  /**
   * 通过库名和表名全量同步数据
   *
   * @param request 请求参数
   */
  @RequestMapping("/byTable")
  @ResponseBody
  public String syncTable(@Validated SyncByTableRequest request) {
    log.debug("request_info: " + JsonUtil.toJson(request));
    String response = Response.success(syncService.syncByTable(request)).toString();
    log.debug("response_info: " + JsonUtil.toJson(request));
    return response;
  }

  @RequestMapping(value = "/all", method = RequestMethod.GET)
  @ResponseBody
  public String syncAll() {
    String response = Response.success(syncService.syncAll()).toString();
    return response;
  }

}
