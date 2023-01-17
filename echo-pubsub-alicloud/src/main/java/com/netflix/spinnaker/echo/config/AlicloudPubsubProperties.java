/*
 * Copyright 2022 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.echo.config;

import com.aliyun.mns.common.utils.ServiceSettings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "pubsub.alicloud")
public class AlicloudPubsubProperties {

  @Valid private List<AlicloudPubsubSubscription> subscriptions;

  @Data
  public static class AlicloudPubsubSubscription {

    private static final Logger log = LoggerFactory.getLogger(AlicloudPubsubSubscription.class);

    /** The name of subscription. */
    @NotEmpty private String name;

    /** The name of mns queue. */
    @NotEmpty private String queueName;

    /** The region of mns service. */
    private String regionId;

    /** The account info of your alibaba cloud. */
    private String accountId;

    private String accessKeyId;
    private String accessKeySecret;

    /** The name of mns topic, which the queue subscribed. */
    private String topicName;

    private String templatePath;

    /** The format of notifyContent, default to NONE. */
    private NotifyContentFormat messageFormat;

    /**
     * Provide an id, present in the message attributes as a string, to use as a unique identifier
     * for processing messages. Fall back to amazon sqs id if alternate Id is not present in the
     * message attributes
     */
    private String alternateIdInMessageAttributes;

    int visibilityTimeout = 30;
    int sqsMessageRetentionPeriodSeconds = 120;
    int waitTimeSeconds = 5;

    // 1 hour default
    private Integer dedupeRetentionSeconds = 3600;

    public AlicloudPubsubSubscription() {}

    public AlicloudPubsubSubscription(
        String name,
        String queueName,
        String regionId,
        String accountId,
        String accessKeyId,
        String accessKeySecret,
        String topicName,
        String templatePath,
        NotifyContentFormat messageFormat,
        String alternateIdInMessageAttributes,
        Integer dedupeRetentionSeconds) {
      this.name = name;
      this.queueName = queueName;
      this.regionId = regionId;
      this.accountId = accountId;
      this.accessKeyId = accessKeyId;
      this.accessKeySecret = accessKeySecret;
      this.topicName = topicName;
      this.templatePath = templatePath;
      this.messageFormat = messageFormat;
      this.alternateIdInMessageAttributes = alternateIdInMessageAttributes;
      if (dedupeRetentionSeconds != null && dedupeRetentionSeconds >= 0) {
        log.warn("AlicloudPubsubProperties initialized!");
        this.dedupeRetentionSeconds = dedupeRetentionSeconds;
      } else {
        if (dedupeRetentionSeconds != null) {
          log.warn("Ignoring dedupeRetentionSeconds invalid value of " + dedupeRetentionSeconds);
        }
      }
      this.init();
    }

    public void init() {
      // While account and region information are not configured, using the default configuration
      // file.
      if (StringUtils.isEmpty(this.accountId) || StringUtils.isEmpty(this.regionId)) {
        String accountEndpoint = ServiceSettings.getMNSAccountEndpoint();
        if (StringUtils.isEmpty(accountEndpoint)) {
          log.error(".aliyun-mns.properties not configured correctly!");
        }
        String[] accountEndpointInfo =
            accountEndpoint.replace("https://", "").replace("http://", "").split("\\.");
        this.accountId = accountEndpointInfo[0];
        this.regionId = accountEndpointInfo[2];
      }
      if (StringUtils.isEmpty(this.accessKeyId)) {
        this.accessKeyId = ServiceSettings.getMNSAccessKeyId();
      }
      if (StringUtils.isEmpty(this.accessKeySecret)) {
        this.accessKeySecret = ServiceSettings.getMNSAccessKeySecret();
      }
    }

    public String getQueueEndpointFormat() {
      return String.format("acs:mns:%s:%s:queues/%s", regionId, accountId, queueName);
    }

    public String getAccountEndpointFormat() {
      return String.format("http://%s.mns.%s.aliyuncs.com", accountId, regionId);
    }

    private NotifyContentFormat determineMessageFormat() {
      // Supplying a custom template overrides a MessageFormat choice
      if (!StringUtils.isEmpty(templatePath)) {
        return NotifyContentFormat.CUSTOM;
      } else if (messageFormat == null) {
        return NotifyContentFormat.NONE;
      }
      return messageFormat;
    }

    public InputStream readTemplatePath() {
      messageFormat = determineMessageFormat();
      log.info(
          "Using message format: {} to process artifacts for subscription: {}",
          messageFormat,
          name);

      try {
        if (messageFormat == NotifyContentFormat.CUSTOM) {
          try {
            return new FileInputStream(new File(templatePath));
          } catch (FileNotFoundException e) {
            // Check if custom jar path was provided before failing
            return getClass().getResourceAsStream(templatePath);
          }
        } else if (messageFormat.jarPath != null) {
          return getClass().getResourceAsStream(messageFormat.jarPath);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to read template in subscription " + name, e);
      }
      return null;
    }
  }

  public static enum NotifyContentFormat {
    OSS("/oss.jinja"),
    CUSTOM(),
    NONE();

    private String jarPath;

    NotifyContentFormat(String jarPath) {
      this.jarPath = jarPath;
    }

    NotifyContentFormat() {}
  }
}
