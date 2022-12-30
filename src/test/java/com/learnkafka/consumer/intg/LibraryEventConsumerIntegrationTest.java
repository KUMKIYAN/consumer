package com.learnkafka.consumer.intg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.consumer.LibraryEventConsumer;
import com.learnkafka.consumer.domain.LibraryEventType;
import com.learnkafka.consumer.entity.Book;
import com.learnkafka.consumer.entity.LibraryEvent;
import com.learnkafka.consumer.repository.FailureRecordRepository;
import com.learnkafka.consumer.repository.LibraryEventRepository;
import com.learnkafka.consumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 2,  brokerProperties = {"listeners=PLAINTEXT://localhost:9099"})
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "retryListener.startup=false"})
public class LibraryEventConsumerIntegrationTest {

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventConsumer libraryEventConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setup(){

        var container = endpointRegistry.getListenerContainers().stream().filter(messageListenerContainer ->
                messageListenerContainer.getGroupId().equals("library-events-listener-group")).collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

/*        for(MessageListenerContainer messageListenerContainer:endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        } */
    }

    @AfterEach
    void tearDown(){
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456," +
                "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(1)).onMessage(any());
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any());

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            System.out.println("*************");
            System.out.println(libraryEvent.getLibraryEventId());
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals (456, libraryEvent.getBook().getBookId());
        });
    }

   @Test
   void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
       String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456," +
               "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

       LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
       libraryEvent.getBook().setLibraryEvent(libraryEvent);
       libraryEventRepository.save(libraryEvent);

       Book book = Book.builder().bookId(456).bookName("Kafka Using Spring Boot V2").bookAuthor("Dillip Kumar").build();

       libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
       libraryEvent.setBook(book);
       String updateJson = objectMapper.writeValueAsString(libraryEvent);
       kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updateJson).get();

       CountDownLatch latch = new CountDownLatch(1);
       latch.await(3, TimeUnit.SECONDS);


       verify(libraryEventConsumerSpy, times(1)).onMessage(any());
       verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(any());

       LibraryEvent persistLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
       assertEquals("Kafka Using Spring Boot V2", persistLibraryEvent.getBook().getBookName());
   }

   @Test
    void publishUpdateLibraryEvent_null_LibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
       String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456," +
               "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
       kafkaTemplate.sendDefault(json).get();

       CountDownLatch latch = new CountDownLatch(1);
       latch.await(3, TimeUnit.SECONDS);

       verify(libraryEventConsumerSpy, times(3)).onMessage(any());
       verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(any());
   }

    @Test
    void publishUpdateLibraryEvent_999_LibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456," +
                "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(3)).onMessage(any());
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(any());

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println("consumer record is :" + consumerRecord.value());
        assertEquals(json, consumerRecord.value());

    }

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent_DeadLetter() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456," +
                "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(3)).onMessage(any());
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(any());

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
        System.out.println("consumer record is :" + consumerRecord.value());
        assertEquals(json, consumerRecord.value());

    }

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent_FailureRecord() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456," +
                "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(999,json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(3)).onMessage(any());
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(any());

        var count = failureRecordRepository.count();
        assertEquals(1, count);

        failureRecordRepository.findAll().forEach(failureRecord -> {
            System.out.println("failureRecord : " + failureRecord);
        });
    }
}
