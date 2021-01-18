package com.risi.mongostreamer.core;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface MongoStreamerRepository extends ReactiveCrudRepository<StreamDetails, String> {
}
