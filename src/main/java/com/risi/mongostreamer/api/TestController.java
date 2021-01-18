package com.risi.mongostreamer.api;

import com.risi.mongostreamer.domain.Player;
import com.risi.mongostreamer.core.TopicRepository;
import com.risi.mongostreamer.repository.PlayerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final PlayerRepository playerRepository;

    @GetMapping(path = "players/{topic}", produces = "application/stream+json")
    public Flux<Player> test(@PathVariable String topic) {
        return playerRepository.findAll(topic);
    }

    @PostMapping("topics/{topic}")
    public Mono<Void> registerTopic(@PathVariable String topic) {
        return playerRepository.registerTopic(topic);
    }

    @GetMapping("topics/{topic}")
    public Mono<Boolean> checkTopicRegistration(@PathVariable String topic) {
        return playerRepository
                .getTopic(topic)
                .map(documentMongoCollection -> true)
                .switchIfEmpty(Mono.just(false));
    }

    @PostMapping("players/{topic}")
    public Mono<Player> savePlayer(@PathVariable String topic, @RequestBody Player player) {
        return playerRepository.save(player, topic);
    }
}
