package rnd.voronkov.kafkatest.model;

import lombok.Data;

@Data
public class Greeting {
    private Long id;
    private String name;
    private String age;
}
