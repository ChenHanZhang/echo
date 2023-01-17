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

package com.netflix.spinnaker.echo.config;

import com.aliyun.mns.client.CloudAccount;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnExpression("${pubsub.enabled:false} && ${pubsub.alicloud.enabled:false}")
public class AlicloudAccountConifg {

  //  CloudAccount account =
  //      new CloudAccount(
  //          ServiceSettings.getMNSAccessKeyId(),
  //          ServiceSettings.getMNSAccessKeySecret(),
  //          ServiceSettings.getMNSAccountEndpoint());
  //
  //  public CloudAccount getDefaultAccount() {
  //    return account;
  //  }

  public CloudAccount newAccount(
      String accessKeyId, String accessKeySecret, String accountEndpoint) {
    return new CloudAccount(accessKeyId, accessKeySecret, accountEndpoint);
  }
}
