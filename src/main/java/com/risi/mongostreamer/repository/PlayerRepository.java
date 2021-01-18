package com.risi.mongostreamer.repository;

import com.risi.mongostreamer.core.EntityAnnotation;
import com.risi.mongostreamer.core.TopicRepository;
import com.risi.mongostreamer.domain.Player;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Repository;

import java.util.Objects;

@EntityAnnotation(entityClass =  Player.class)
@Repository
public class PlayerRepository extends TopicRepository<Player> {

    public PlayerRepository() {
        entityClass =
                Objects.requireNonNull(AnnotationUtils.findAnnotation(aClass, EntityAnnotation.class)).entityClass();
    }
}
