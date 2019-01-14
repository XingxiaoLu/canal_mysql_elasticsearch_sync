package com.star.sync.elasticsearch.controller.handler;

import javax.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import com.star.sync.elasticsearch.model.response.Response;
import lombok.extern.slf4j.Slf4j;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-31 20:55:00
 */
@ControllerAdvice
@Slf4j
public class ControllerHandler {

  @ExceptionHandler
  @ResponseBody
  public Object exceptionHandler(Exception e, HttpServletResponse response) {
    log.error("unknown_error", e);
    return new Response<>(2, e.getMessage(), null).toString();
  }
}
