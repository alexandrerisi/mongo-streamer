package com.risi.mongostreamer;

import com.risi.mongostreamer.domain.Player;
import com.risi.mongostreamer.repository.PlayerRepository;
import com.risi.mongostreamer.service.TopicService;
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
