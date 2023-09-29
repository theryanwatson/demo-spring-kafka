package org.watson.demos.configurations;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@ConditionalOnProperty(value = "spring.cache.enabled", matchIfMissing = true)
@EnableCaching
@Configuration(proxyBeanMethods = false)
public class CachingConfiguration {
    @Bean
    public Cache messageCache(final CacheManager cacheManager) {
        return cacheManager.getCache("message.fake.fail.count.cache");
    }
}
