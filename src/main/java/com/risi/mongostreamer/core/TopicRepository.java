package com.risi.mongostreamer.core;

import com.mongodb.ConnectionString;
import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.mongo.ReactiveMongoClientFactory;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoClientFactoryBean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.springframework.data.mongodb.core.query.Query.query;

@SuppressWarnings("unchecked")
@Component
@NoArgsConstructor
public abstract class TopicRepository<T extends MongoStreamerEntity> {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    @Value("${mongo.streamer.default.collection.size}")
    private long defaultSize;
    @Value("${mongo.streamer.default.collection.max.doc}")
    private long maxDocuments;
    @Value("${mongo.streamer.extension.name}")
    private String extensionName;

    private ReactiveMongoOperations mongoOperations;
    private ReactiveMongoTemplate template;
    private MongoStreamerRepository streamerRepository;
    protected final Class<?> aClass = this.getClass();
    protected Class entityClass;
    private CopyOnWriteArraySet<StreamDetails> registeredTopics = new CopyOnWriteArraySet<>();

    @Data
    class TailableEntity<T> {

        private Disposable subscription;
        private Flux<T> stream;

        public TailableEntity(String topic) {
            stream = template.tail(
                    new Query(),
                    entityClass,
                    topic + extensionName);
            subscription = stream.subscribe();
        }

        @PreDestroy
        public void close() {
            subscription.dispose();
        }
    }

    @Tailable
    public Flux<T> findAll(String topic) {

        /*ConnectionString connString = new ConnectionString(
                "mongodb+srv://streamer_user:streamer_user@cluster0.t0q9n.mongodb.net/test?retryWrites=true"
        );
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .applyToConnectionPoolSettings(builder -> builder.maxSize(1).minSize(1))
                .retryWrites(true)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("streamer-db");

        return Flux.from(database.getCollection(topic + "_topic").find().cursorType(CursorType.Tailable))
                .doOnCancel(mongoClient::close);*/

        var streamSubscription = new Subscription[1];
        return template
                .tail(null, entityClass, topic + extensionName)
                .log()
                .doOnSubscribe(subscription -> {
                    logger.log(Level.INFO, "Initiating stream from topic = " + topic);
                    streamSubscription[0] = (Subscription) subscription;
                })
                .doOnCancel(() -> {
                    logger.log(Level.INFO, "Terminating stream from topic = " + topic);
                    streamSubscription[0].cancel();
                });
    }

    public Mono<T> findById(String id, String topic) {

        return template.findById(id, entityClass, topic + extensionName);
    }

    public Mono<T> save(T el, String topic) {
        //if (registeredTopics.contains(new StreamDetails(topic + extensionName)))
            return template.save(el, topic + extensionName);

        //throw new RuntimeException();
    }

    public Mono<Void> delete(T el, String topic) {
        if (registeredTopics.contains(new StreamDetails(topic + extensionName)))
            return template.remove(el, topic + extensionName)
                    .then();

        throw new RuntimeException();
    }

    public Mono<Void> deleteById(String id, String topic) {
        return findById(id, topic)
                .doOnNext(t -> delete(t, topic))
                .then();
    }

    public Mono<Void> registerTopic(String topicName) {

        var collectionId = topicName + extensionName;

        return streamerRepository
                .findById(collectionId)
                .flatMap(streamDetails -> Mono.error(new RuntimeException()))
                .switchIfEmpty(
                        template.createCollection(collectionId, defaultCollectionOptions())
                                .flatMap(documentMongoCollection -> streamerRepository
                                        .save(new StreamDetails(collectionId)))
                                .doOnNext(streamDetails -> registeredTopics.add(streamDetails))
                )
                .then();
    }

    public Mono<MongoCollection<Document>> getTopic(String topic) {
        return template.getCollection(topic);
    }

    public Mono<Void> removeTopic(String topicName) {
        var collectionId = topicName + extensionName;

        return streamerRepository
                .findById(collectionId)
                .flatMap(streamDetails -> streamerRepository.delete(streamDetails));
    }

    @Autowired
    public void setTemplate(ReactiveMongoTemplate template) {
        this.template = template;
    }

    @Autowired
    public void setStreamerRepository(MongoStreamerRepository streamerRepository) {
        this.streamerRepository = streamerRepository;
    }

    public CollectionOptions defaultCollectionOptions() {
        return CollectionOptions.empty()
                .size(defaultSize)
                .maxDocuments(maxDocuments)
                .capped();
    }
}
