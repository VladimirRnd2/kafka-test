package rnd.voronkov.kafkatest.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class Goodbye {
    private Long id;
    private String name;
    private String age;
}
