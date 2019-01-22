/**
 * 
 */
package com.star.sync.elasticsearch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

/**
 * @author luxingxiao
 *
 */
@ConfigurationProperties(prefix = "canal")
@Data
public class CanalProperties {
  private String host;
  private String port;
  private String destinations;
  private String username;
  private String password;
}
