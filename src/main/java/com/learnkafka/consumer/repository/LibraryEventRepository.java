package com.learnkafka.consumer.repository;

import com.learnkafka.consumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {
}
