package com.risi.mongostreamer.core;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

@Data
@NoArgsConstructor
@EqualsAndHashCode
public class StreamDetails {

    @Id
    private String id;

    public StreamDetails(String id) {
        this.id = id;
    }
}
