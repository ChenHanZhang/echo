/*
 * Copyright 2023 Netflix, Inc.
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

package com.netflix.spinnaker.echo.pubsub.alicloud;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationMessageWrapper {

  @JsonProperty("TopicOwner")
  private String topicOwner;

  @JsonProperty("Subscriber")
  private String subscriber;

  @JsonProperty("TopicName")
  private String topicName;

  @JsonProperty("SubscriptionName")
  private String subscriptionName;

  @JsonProperty("Message")
  private String message;

  @JsonProperty("MessageId")
  private String messageId;

  @JsonProperty("MessageMD5")
  private String messageMD5;

  @JsonProperty("PublishTime")
  private String publishTime;

  public NotificationMessageWrapper() {}

  public NotificationMessageWrapper(
      String topicOwner,
      String subscriber,
      String topicName,
      String subscriptionName,
      String message,
      String messageId,
      String messageMD5,
      String publishTime,
      Map<String, String> messageAttributes) {
    this.topicOwner = topicOwner;
    this.subscriber = subscriber;
    this.topicName = topicName;
    this.subscriptionName = subscriptionName;
    this.message = message;
    this.messageId = messageId;
    this.messageMD5 = messageMD5;
    this.publishTime = publishTime;
  }
}
