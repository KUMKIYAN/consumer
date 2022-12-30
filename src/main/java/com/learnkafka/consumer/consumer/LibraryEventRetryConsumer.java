package com.learnkafka.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.entity.LibraryEvent;
import com.learnkafka.consumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventRetryConsumer {

    @Autowired
    LibraryEventsService libraryEventsService;

    @KafkaListener(topics = {"${topics.retry}"}, groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer Record in Retry Consumer: {}  ", consumerRecord);
        consumerRecord.headers().forEach( header -> {
            log.info("key :{} , value: {}", header.key(), new String(header.value()));
        });
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
