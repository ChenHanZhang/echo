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

package com.netflix.spinnaker.echo.pubsub.alicloud;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.model.Message;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.echo.pubsub.model.MessageAcknowledger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responds to the MNS queue for each message using the unique messageReceiptHandle of the message.
 */
public class AlicloudMessageAcknowledger implements MessageAcknowledger {

  private static final Logger log = LoggerFactory.getLogger(AlicloudMessageAcknowledger.class);

  private final String subscriptionName;
  private final CloudQueue cloudQueue;
  private final Message message;
  private final Registry registry;

  public AlicloudMessageAcknowledger(
      CloudQueue cloudQueue, String subscriptionName, Message message, Registry registry) {
    this.cloudQueue = cloudQueue;
    this.subscriptionName = subscriptionName;
    this.message = message;
    this.registry = registry;
  }

  @Override
  public void ack() {
    // Delete from queue
    try {
      cloudQueue.deleteMessage(message.getReceiptHandle());
      registry.counter(getProcessedMetricId(subscriptionName)).increment();
    } catch (ReceiptHandleIsInvalidException e) {
      log.warn(
          "Error deleting message: {}, queue: {}, reason: {} (receiptHandle: {})",
          message.getMessageId(),
          subscriptionName,
          e.getMessage(),
          message.getReceiptHandle());
    }
  }

  @Override
  public void nack() {
    // Do nothing. Message is being processed by another worker,
    // and will be available again in 30 seconds to process
    registry.counter(getNackMetricId(subscriptionName)).increment();
  }

  Id getProcessedMetricId(String subscriptionName) {
    return registry.createId(
        "echo.pubsub.alicloud.totalProcessed", "subscriptionName", subscriptionName);
  }

  Id getNackMetricId(String subscriptionName) {
    return registry.createId(
        "echo.pubsub.alicloud.messagesNacked", "subscriptionName", subscriptionName);
  }
}
