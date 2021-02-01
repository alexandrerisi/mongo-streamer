package com.risi.mongostreamer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.risi.mongostreamer.domain.Player;
import com.risi.mongostreamer.repository.PlayerRepository;
import com.risi.mongostreamer.service.TopicService;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SpringBootApplication
public class MongoStreamerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MongoStreamerApplication.class, args);
    }

    //@Bean
    public CommandLineRunner runner(ReactiveMongoOperations template, TopicService streamerService, PlayerRepository repository) {

        ConnectionString connString = new ConnectionString(
                "mongodb+srv://streamer_user:streamer_user@cluster0.t0q9n.mongodb.net/test?retryWrites=true"
        );
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .retryWrites(true)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("");
        database.runCommand(new Document());

        return args -> {
            var topic = "test";
            streamerService.registerTopic(topic).block();
            streamerService.getTopic(topic).block();

            template.getCollectionNames().subscribe(System.out::println);

            Flux.interval(Duration.ofSeconds(5))
                    .flatMap(aLong -> template.save(
                            new Player(null, Integer.parseInt(aLong + ""), "as", "asd"), topic + "_topic")
                    )
                    .subscribe(player -> System.out.println("recording -> " + player));
        };
    }
}
