package com.dxc.quartorariecsvgenerator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;


import static java.lang.System.exit;

@SpringBootApplication
public class QuartorarieCsvGeneratorApplication implements CommandLineRunner {

    @Autowired
    private MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(QuartorarieCsvGeneratorApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java -jar QuartorarieCsvGeneratorApplication.jar magnitude fileName");
            exit(1);
        }
        Query query = new Query();
        long count = mongoTemplate.count(query, "curveQuartorarie");
        System.out.println("Numero di record nella collezione curveQuartorarie: " + count);
    }
}
