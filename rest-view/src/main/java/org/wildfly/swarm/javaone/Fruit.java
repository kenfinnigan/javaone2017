package org.wildfly.swarm.javaone;

/**
 * @author Ken Finnigan
 */
public class Fruit {
    private Integer id;

    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Fruit named `" + name + "` with id of: " + id;
    }

}
