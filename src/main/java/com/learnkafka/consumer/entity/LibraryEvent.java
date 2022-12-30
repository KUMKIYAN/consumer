package com.learnkafka.consumer.entity;


import com.learnkafka.consumer.domain.LibraryEventType;
import lombok.*;
import org.springframework.boot.autoconfigure.domain.EntityScan;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {
    @Id
    @GeneratedValue
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;
}
