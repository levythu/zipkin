/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.autoconfigure.storage.deltafs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin.storage.StorageComponent;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.deltafs.DeltaFSStorage;


@Configuration
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "deltafs")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinDeltaFSStorageAutoConfiguration {

    @Bean StorageComponent storage(
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
      @Value("${zipkin.storage.mem.max-spans:500000}") int maxSpans) {
      return DeltaFSStorage.builder()
        .strictTraceId(strictTraceId)
        .build();
    }

}
