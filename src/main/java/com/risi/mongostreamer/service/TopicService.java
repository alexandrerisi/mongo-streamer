package com.risi.mongostreamer.service;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.risi.mongostreamer.core.MongoStreamerRepository;
import com.risi.mongostreamer.core.StreamDetails;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class TopicService {

    @Value("${mongo.streamer.default.collection.size}")
    private long defaultSize;
    @Value("${mongo.streamer.default.collection.max.doc}")
    private long maxDocuments;
    @Value("${mongo.streamer.extension.name}")
    private String extensionName;
    private MongoStreamerRepository streamerRepository;
    private ReactiveMongoOperations mongoOperations;

    public TopicService(MongoStreamerRepository streamerRepository,
                        ReactiveMongoOperations mongoOperations) {

        this.mongoOperations = mongoOperations;
        this.streamerRepository = streamerRepository;
    }

    public Mono<Void> registerTopic(String topicName) {

        var collectionId = topicName + extensionName;

        return streamerRepository
                .findById(collectionId)
                .flatMap(streamDetails -> Mono.error(new RuntimeException()))
                .switchIfEmpty(
                        mongoOperations.createCollection(collectionId, defaultCollectionOptions())
                                .flatMap(documentMongoCollection -> streamerRepository
                                        .save(new StreamDetails(collectionId)))
                )
                .then();
    }

    public Mono<MongoCollection<Document>> getTopic(String streamerId) {
        
        var collectionId = streamerId + extensionName;

        return streamerRepository
                .findById(collectionId)
                .flatMap(streamDetails -> mongoOperations.getCollection(collectionId))
                .switchIfEmpty(Mono.error(new RuntimeException()));
    }

    public CollectionOptions defaultCollectionOptions() {
        return CollectionOptions.empty()
                .size(defaultSize)
                .maxDocuments(maxDocuments)
                .capped();
    }
}
