package org.wildfly.swarm.javaone;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.KafkaConfig;
import net.wessendorf.kafka.cdi.annotation.Producer;

/**
 * @author Ken Finnigan
 */
@Path("/fruits")
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class FruitResource {

    @PersistenceContext(unitName = "FruitPU")
    private EntityManager em;

    @Producer
    private SimpleKafkaProducer<Integer, Fruit> producer;

    @GET
    @Produces("application/json")
    public Fruit[] get() {
        return em
                .createNamedQuery("Fruits.findAll", Fruit.class)
                .getResultList()
                .toArray(new Fruit[0]);
    }

    @GET
    @Path("/{id}")
    @Produces("application/json")
    public Fruit getSingle(@PathParam("id") Integer id) {
        return em.find(Fruit.class, id);
    }


    @POST
    @Consumes("application/json")
    @Produces("application/json")
    @Transactional
    public Response create(Fruit fruit) {
        if (fruit.getId() != null) {
            return error(422, "Id was invalidly set on request.");
        }

        try {
            em.persist(fruit);
        } catch (Exception e) {
            return error(500, e.getMessage());
        }

        producer.send("fruit_topic", fruit.getId(), fruit);

        return Response.ok(fruit).status(201).build();
    }

    @PUT
    @Path("/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @Transactional
    public Response update(@PathParam("id") Integer id, Fruit fruit) {
        if (fruit.getName() == null) {
            return error(422, "Fruit Name was not set on request.");
        }

        try {
            Fruit entity = em.find(Fruit.class, id);

            if (entity == null) {
                return error(404, "Fruit with id of " + id + " does not exist.");
            }

            entity.setName(fruit.getName());
            em.merge(entity);

            producer.send("fruit_topic", entity.getId(), entity);

            return Response.ok(entity).status(200).build();
        } catch (Exception e) {
            return error(500, e.getMessage());
        }
    }

    private Response error(int code, String message) {
        return Response
                .status(code)
                .entity(Json.createObjectBuilder()
                                .add("error", message)
                                .add("code", code)
                                .build()
                )
                .build();
    }
}
