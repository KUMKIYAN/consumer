package com.learnkafka.consumer.service;

import com.learnkafka.consumer.entity.FailureRecord;
import com.learnkafka.consumer.repository.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String recordStatus){
        var failedRecord = new FailureRecord(null, consumerRecord.topic(), consumerRecord.key(), consumerRecord.value(),
                consumerRecord.partition(), consumerRecord.offset(), e.getMessage(), recordStatus);
        log.info("inside before save Failed record with status : " + recordStatus);
        failureRecordRepository.save(failedRecord);
        log.info("inside after save Failed record with status : " + recordStatus);
    }
}
