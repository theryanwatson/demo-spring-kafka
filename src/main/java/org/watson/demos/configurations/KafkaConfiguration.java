package org.watson.demos.configurations;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {}
