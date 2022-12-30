package com.learnkafka.consumer.entity;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class FailureRecord {
    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer messageKey;
    private String errorRecord;
    private Integer partition;
    private Long offset_value;
    private String exception;
    private String status;
}
