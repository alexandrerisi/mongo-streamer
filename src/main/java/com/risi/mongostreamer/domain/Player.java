package com.risi.mongostreamer.domain;

import com.risi.mongostreamer.core.MongoStreamerEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Player implements MongoStreamerEntity {

    private String id;
    private int age;
    private String name;
    private String email;
}
