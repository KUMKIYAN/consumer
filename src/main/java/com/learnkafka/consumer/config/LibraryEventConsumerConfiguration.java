package com.learnkafka.consumer.config;

import com.learnkafka.consumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfiguration {

    private static final String RETRY = "RETRY";
    private static final String DEAD = "DEAD";


    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;

//    @Value("${topics.retry}")
//    private String retryTopic;
//
//    @Value("${topics.dlt}")
//    private String deadLetterTopic;


    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;


    private final int MAX_RETRY_ATTEMPTS = 3;

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {

        var record = (ConsumerRecord<Integer, String>) consumerRecord;
        log.error("Exception in publishing Recoverer : {}", e.getMessage(), e);
        if (e.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside Recovery");
            failureService.saveFailedRecord( record, e, "RETRY");
        }
        else {
            log.info("Inside non-Recovery");
            failureService.saveFailedRecord(record, e, "DEAD");
        }
    };


    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                log.error("Inside Recoverable Data Access Exception block");
                return new TopicPartition(retryTopic, r.partition());
            } else {
                log.error("Inside Dead letter topic block");
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });
        return recoverer;
    }

    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, MAX_RETRY_ATTEMPTS);
        // var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);

        var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);
        var expBackOff = new ExponentialBackOff();
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);
        // var errorHandler =  new DefaultErrorHandler(fixedBackOff);
        // var errorHandler =  new DefaultErrorHandler(expBackOff);
        //var errorHandler = new DefaultErrorHandler(publishingRecoverer(), fixedBackOff);
        var errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, fixedBackOff);
        // exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);
        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt);
        }));
        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        //  factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
