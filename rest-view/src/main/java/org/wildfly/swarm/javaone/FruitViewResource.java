package org.wildfly.swarm.javaone;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.annotation.KafkaConfig;

/**
 * @author Ken Finnigan
 */
@Path("/")
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class FruitViewResource {
    Map<Integer, Fruit> fruitMap = new HashMap<Integer, Fruit>();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<Fruit> allFruit() {
        return fruitMap.values();
    }

    @Consumer(topics = "fruit_topic", keyType = Integer.class, groupId = "inmemory", offset = "earliest")
    public void processFruit(Integer key, Fruit value) {
        this.fruitMap.put(key, value);
    }
}
