package com.learnkafka.consumer.repository;

import com.learnkafka.consumer.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {
}
