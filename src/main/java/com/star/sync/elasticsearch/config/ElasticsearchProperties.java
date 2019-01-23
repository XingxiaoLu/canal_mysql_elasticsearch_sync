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
@ConfigurationProperties(prefix = "elasticsearch")
@Data
public class ElasticsearchProperties {
  private String pemcertFilePath;
  private String pemkeyFilePath;
  private String pemTrustedcasFilePath;
  private String clustername;
  private String host;
  private String port;
  private String username;
  private String password;
}
