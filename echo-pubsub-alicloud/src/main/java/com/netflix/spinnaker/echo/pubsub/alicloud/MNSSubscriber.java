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

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.echo.config.AlicloudPubsubProperties;
import com.netflix.spinnaker.echo.model.pubsub.MessageDescription;
import com.netflix.spinnaker.echo.model.pubsub.PubsubSystem;
import com.netflix.spinnaker.echo.pubsub.PubsubMessageHandler;
import com.netflix.spinnaker.echo.pubsub.model.PubsubSubscriber;
import com.netflix.spinnaker.echo.pubsub.utils.NodeIdentity;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * One subscriber for each subscription. The subscriber makes sure the MNS queue is created,
 * subscribes to the MNS topic, polls the queue for messages, and removes them once processed.
 */
public class MNSSubscriber implements Runnable, PubsubSubscriber {

  private static final Gson gson = new Gson();
  private static final Logger logger = LoggerFactory.getLogger(MNSSubscriber.class);

  private static final PubsubSystem pubsubSystem = PubsubSystem.ALICLOUD;
  private static final int MNS_MAX_NUMBER_OF_MESSAGES = 10;
  private static final String DEFAULT_CHARSET = "UTF-8";

  private final ObjectMapper objectMapper;
  private final AlicloudPubsubProperties.AlicloudPubsubSubscription subscription;
  private final PubsubMessageHandler pubsubMessageHandler;
  private final NodeIdentity identity = new NodeIdentity();

  private final MNSClient mnsClient;
  private CloudTopic cloudTopic = null;
  private CloudQueue cloudQueue = null;

  private final String topicName;
  private final String queueName;

  private final Supplier<Boolean> isEnabled;

  private final Registry registry;

  public MNSSubscriber(
      ObjectMapper objectMapper,
      AlicloudPubsubProperties.AlicloudPubsubSubscription subscription,
      PubsubMessageHandler pubsubMessageHandler,
      MNSClient mnsClient,
      Supplier<Boolean> isEnabled,
      Registry registry) {
    this.objectMapper = objectMapper;
    this.subscription = subscription;
    this.pubsubMessageHandler = pubsubMessageHandler;
    this.mnsClient = mnsClient;
    this.isEnabled = isEnabled;
    this.registry = registry;
    this.topicName = subscription.getTopicName();
    this.queueName = subscription.getQueueName();
  }

  public String getWorkerName() {
    return subscription.getName() + "/" + MNSSubscriber.class.getSimpleName();
  }

  @Override
  public PubsubSystem getPubsubSystem() {
    return pubsubSystem;
  }

  @Override
  public String getSubscriptionName() {
    return subscription.getName();
  }

  @Override
  public String getName() {
    return getSubscriptionName();
  }

  @Override
  public void run() {
    logger.info("Starting " + getWorkerName());
    try {
      initializeQueue();
    } catch (Exception e) {
      logger.error("Error initializing MNS queue {}", queueName, e);
      throw e;
    }

    while (true) {
      try {
        listenForMessages();
      } catch (ServiceException e) {
        logger.warn("Queue {} does not exist, recreating", queueName);
        initializeQueue();
      } catch (Exception e) {
        logger.error("Unexpected error running " + getWorkerName() + ", restarting worker", e);
        sleepALittle();
      }
    }
  }

  private void initializeQueue() {
    // Ensure that the queue has been created.
    cloudQueue = ensureQueueExists();

    if (topicName != null && !topicName.equals("")) {
      // Ensure that the topic has been created.
      cloudTopic = ensureTopicExists();
      // Make the queue subscribe messages produced from topic.
      SubscriptionMeta subMeta = new SubscriptionMeta();
      subMeta.setSubscriptionName(subscription.getQueueName());
      subMeta.setEndpoint(subscription.getQueueEndpointFormat());
      subMeta.setNotifyContentFormat(SubscriptionMeta.NotifyContentFormat.JSON);
      cloudTopic.subscribe(subMeta);
    }
  }

  private void listenForMessages() {

    while (isEnabled.get()) {
      // TODO: Consider consumption parameter of the queue.
      List<Message> batchPeekMessage = null;
      try {
        batchPeekMessage = cloudQueue.batchPopMessage(MNS_MAX_NUMBER_OF_MESSAGES);
      } catch (ClientException e) {
        // If something exception occurs, let's sleep a little.
        sleepALittle();
      }

      if (CollectionUtils.isEmpty(batchPeekMessage)) {
        logger.debug("Received no messages for queue: {}", queueName);
        continue;
      }

      batchPeekMessage.forEach(this::handleMessage);
    }

    // If isEnabled is false, let's not busy spin.
    sleepALittle();
  }

  private void handleMessage(Message message) {
    try {
      String messageId = message.getMessageId();
      // Try to parse message body, echo assume it's a JSON-formatted string.
      // The message body may be derived from the topic and therefore needs to be parsed.
      String messageBody = unmarshalMessageBody(getOriginalMessageBody(message));
      // The data in the message body may be encoded.
      String messageBodyDecoded = getDecodedMessageBody(messageBody);
      /// Try to parse message payload.
      String messagePayload = parseMessagePayload(messageBodyDecoded);
      // Try to parse message attributes.
      HashMap<String, String> messageAttributes =
          new HashMap<>(parseMessageAttributes(messageBodyDecoded));

      // When message attributes and message carriers cannot be resolved,
      // the information passed should come from a third party system.
      if (CollectionUtils.isEmpty(messageAttributes) && messagePayload.equals(messageBodyDecoded)) {
        HashMap<String, String> wrapper = gson.fromJson(messagePayload, HashMap.class);
        messageAttributes.putAll(
            wrapper.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue()))));
        // messagePayload = gson.toJson(messageAttributes);
      }

      // TODO: Topic message attributes maybe stored within the Queue message body. Add them to
      // other attributes.

      MessageDescription description =
          MessageDescription.builder()
              .subscriptionName(getSubscriptionName())
              .messagePayload(messagePayload)
              .messageAttributes(messageAttributes)
              .pubsubSystem(pubsubSystem)
              .ackDeadlineSeconds(60)
              .retentionDeadlineSeconds(subscription.getDedupeRetentionSeconds())
              .build();

      AlicloudMessageAcknowledger acknowledger =
          new AlicloudMessageAcknowledger(cloudQueue, getName(), message, registry);

      if (subscription.getAlternateIdInMessageAttributes() != null
          && !subscription.getAlternateIdInMessageAttributes().isEmpty()
          && messageAttributes.containsKey(subscription.getAlternateIdInMessageAttributes())) {
        // Message attributes contain the unique id used for dedupe message.
        messageId = messageAttributes.get(subscription.getAlternateIdInMessageAttributes());
      }

      pubsubMessageHandler.handleMessage(
          description, acknowledger, identity.getIdentity(), messageId);
    } catch (Exception e) {
      registry.counter(getFailedToBeHandledMetricId(e)).increment();
      logger.error("Message {} from queue {} failed to be handled", message, getName(), e);
      // Todo emjburns: add dead-letter queue policy
    }
  }

  private String getOriginalMessageBody(Message message) {
    String messageBody = message.getMessageBodyAsRawString();
    try {
      Map wrapper = gson.fromJson(messageBody, Map.class);
      if (wrapper != null && !wrapper.isEmpty()) {
        return messageBody;
      }
    } catch (Exception e) {
      // While parse error, it may be encoded by Base64 encoder.
      messageBody = message.getMessageBody();
    }
    return messageBody;
  }

  // We assume that the body of the message is a json string, if not, try to decode by base64.
  private String getDecodedMessageBody(String messagePayload) {

    try {
      Map wrapper = gson.fromJson(messagePayload, Map.class);
      if (wrapper != null && !wrapper.isEmpty()) {
        return messagePayload;
      }
    } catch (Exception e) {
      // While parse error, it may be encoded by Base64 encoder.
    }

    try {
      messagePayload =
          new String(Base64.decodeBase64(messagePayload), MNSSubscriber.DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // If a processing error occurs, it may be resolved by the upper application.
    }

    return messagePayload;
  }

  /**
   * Messages in the queue may originate from topics, so before parse the message we try to
   * unmarshal messageBody.
   */
  private String unmarshalMessageBody(String messageBody) {
    String messagePayload = messageBody;
    try {
      NotificationMessageWrapper wrapper =
          objectMapper.readValue(messagePayload, NotificationMessageWrapper.class);
      if (wrapper != null && wrapper.getMessage() != null) {
        messagePayload = wrapper.getMessage();
      }
    } catch (IOException e) {
      // Try to unwrap a notification message; if that doesn't work,
      // we're dealing with a message we can't parse. The template or
      // the pipeline potentially knows how to deal with it.
      logger.error(
          "Unable unmarshal NotificationMessageWrapper. Unknown message type. (body: {})",
          messageBody,
          e);
    }
    return messagePayload;
  }

  private String parseMessagePayload(String messageBody) {
    try {
      MessageBodyWrapper messageBodyWrapper = gson.fromJson(messageBody, MessageBodyWrapper.class);
      if (messageBodyWrapper.getMessagePayload() != null) {
        return messageBodyWrapper.getMessagePayload();
      }
    } catch (Exception e) {
      logger.error(
          "Unable unmarshal MessageBodyWrapper. Unknown message content. (contnet: {})",
          messageBody,
          e);
    }
    // Messages delivered from other systems remain in their original format.
    return messageBody;
  }

  /**
   * Although message delivery from mns did not provide message attributes, we assume that the user
   * is using JSON-formatted messages contains attributes fields.
   */
  private Map<String, String> parseMessageAttributes(String messageBody) {
    try {
      MessageBodyWrapper wrapper = gson.fromJson(messageBody, MessageBodyWrapper.class);
      if (wrapper != null
          && wrapper.getMessageAttribute() != null
          && !wrapper.getMessageAttribute().isEmpty()) {
        return wrapper.getMessageAttribute();
      }
    } catch (Exception e) {
      logger.error(
          "Unable unmarshal NotificationMessageWrapper. Unknown message type. (body: {})",
          messageBody,
          e);
    }
    return Collections.emptyMap();
  }

  /** Create queue if there is no one exist. */
  private CloudQueue ensureQueueExists() {
    CloudQueue queue;

    try {
      List<QueueMeta> queueMetas =
          Optional.ofNullable(mnsClient.listQueue(queueName, null, 1))
              .map(PagingListResult::getResult)
              .orElse(new ArrayList<>());
      if (queueMetas.isEmpty()) {
        throw new ServiceException("Cannot find queue {}", queueName);
      }
      queue = mnsClient.createQueue(queueMetas.get(0));
      logger.debug("Reusing existing queue {}", queue);
    } catch (ServiceException e) {
      QueueMeta meta = new QueueMeta();
      meta.setQueueName(queueName);
      queue = mnsClient.createQueue(meta);
      logger.debug("Created queue {}", queue);
    }

    // TODO: Configure features of queue.
    QueueMeta newMeta = new QueueMeta();
    newMeta.setQueueName(queueName);
    newMeta.setDelaySeconds(30L);
    queue.setAttributes(newMeta);
    return queue;
  }

  private CloudTopic ensureTopicExists() {
    CloudTopic topic;

    try {
      List<TopicMeta> topicMetas =
          Optional.ofNullable(mnsClient.listTopic(topicName, null, 1))
              .map(PagingListResult::getResult)
              .orElse(new ArrayList<>());
      if (topicMetas.isEmpty()) {
        throw new ServiceException("Cannot find topic {}", topicName);
      }
      topic = mnsClient.createTopic(topicMetas.get(0));
      logger.debug("Reusing existing topic {}", topic);
    } catch (ServiceException e) {
      TopicMeta meta = new TopicMeta();
      meta.setTopicName(topicName);
      topic = mnsClient.createTopic(meta);
      logger.debug("Created topic {}", topic);
    }

    // TODO: Configure features of topic.
    return topic;
  }

  private Id getFailedToBeHandledMetricId(Exception e) {
    return registry
        .createId("echo.pubsub.alicloud.failedMessages")
        .withTag("exceptionClass", e.getClass().getSimpleName());
  }

  private void sleepALittle() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e1) {
      logger.error("Thread {} interrupted while sleeping", getWorkerName(), e1);
    }
  }
}
