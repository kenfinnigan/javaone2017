package org.wildfly.swarm.javaone;

import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.annotation.KafkaConfig;
import org.jboss.logging.Logger;

/**
 * @author Ken Finnigan
 */
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class FruitConsumer {
    private Logger logger = Logger.getLogger(FruitConsumer.class.getName());

    @Consumer(topics = "fruit_topic", keyType = Integer.class, groupId = "fruit_processor")
    public void processFruit(final Integer key, final Fruit fruitData) {
        logger.error("We received: " + fruitData);
    }
}
