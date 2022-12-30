package com.learnkafka.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.entity.LibraryEvent;
import com.learnkafka.consumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventConsumer{

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    ObjectMapper objectMapper;

    @KafkaListener(topics = {"library-events"}, groupId = "library-events-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("inside main topic");
        log.info("Consumer Record : {}  ", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
