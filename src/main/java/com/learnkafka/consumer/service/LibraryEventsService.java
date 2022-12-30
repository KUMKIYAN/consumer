package com.learnkafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.entity.LibraryEvent;
import com.learnkafka.consumer.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEvent : {} ", libraryEvent);

        if(libraryEvent !=null && (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999)){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null ){
            throw new IllegalArgumentException("Invalid Library Event Type");
        }

        Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if(!optionalLibraryEvent.isPresent()){
            throw new IllegalArgumentException("Not a valid library event Id");
        }
        log.info("Validation is successful for the library Event: {} ", optionalLibraryEvent.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Successfully Persisted the library Event : {} ", libraryEvent);
    }

}
