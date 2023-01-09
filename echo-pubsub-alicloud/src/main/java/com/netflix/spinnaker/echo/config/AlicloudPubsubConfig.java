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

import com.netflix.spinnaker.kork.aws.bastion.BastionConfig;
import javax.annotation.PostConstruct;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(BastionConfig.class)
@ConditionalOnExpression("${pubsub.enabled:false} && ${pubsub.alicloud.enabled:false}")
@EnableConfigurationProperties(AlicloudPubsubProperties.class)
public class AlicloudPubsubConfig {

  @Valid @Autowired private AlicloudPubsubProperties alicloudPubsubProperties;

  private static final Logger log = LoggerFactory.getLogger(AlicloudPubsubConfig.class);

  @PostConstruct
  public void start() {
    log.debug("AlicloudPubsubConfig initialized!");
    log.debug("{}:", alicloudPubsubProperties);
  }
}
