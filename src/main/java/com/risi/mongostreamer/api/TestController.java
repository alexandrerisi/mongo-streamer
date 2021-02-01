package com.risi.mongostreamer.api;

import com.risi.mongostreamer.domain.Player;
import com.risi.mongostreamer.core.TopicRepository;
import com.risi.mongostreamer.repository.PlayerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final PlayerRepository playerRepository;
    private final ReactiveMongoTemplate template;
    private Disposable subscription;

    @GetMapping(path = "players/{topic}", produces = "application/stream+json")
    public Flux test(@PathVariable String topic) {
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

    @GetMapping("/start")
    public void startStream() {
        var stream = template.tail(null, Player.class, "myTopic_topic");
        subscription = stream.subscribe(System.out::println);
    }

    @GetMapping("/stop")
    public void stopStream() {
        subscription.dispose();
        System.out.print(subscription.isDisposed());
    }
}
