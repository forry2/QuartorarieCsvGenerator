package com.dxc.quartorariecsvgenerator;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.exit;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@SpringBootApplication
public class QuartorarieCsvGeneratorApplication implements CommandLineRunner {

    @Autowired
    private MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(QuartorarieCsvGeneratorApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: java -jar QuartorarieCsvGeneratorApplication.jar magnitude fileName startDate endDate");
            exit(1);
        }

        String magnitude = args[0];
        String fileName = args[1];
        String startDate = args[2];
        String endDate = args[3];


        AggregationOperation project = project()
                .and("MEAS_YMDD_ID").as("MEAS_YMDD_ID")
                .and("dataFilenameMagnitude").as("dataFilenameMagnitude")
                .and("POD").as("POD")
                .and("MEAS_TYPE").as("MEAS_TYPE")
                .and("val").as("val")
                .andExclude("_id");

        AggregationOperation match = match(
                where("MEAS_YMDD_ID").gte(startDate).lt(endDate)
                        .and("dataFilenameMagnitude").is(fileName + "_" + magnitude)
        );

        AggregationOperation distinctPodGroupByPod = group("POD");

        AggregationOperation distinctPodSortByPod = sort(Sort.by("_id"));

        Aggregation distinctPodAggregation = Aggregation.newAggregation(project, match, distinctPodGroupByPod, distinctPodSortByPod);
        AggregationResults<Document> distinctPod = mongoTemplate.aggregate(
                distinctPodAggregation,
                "curveQuartorarie",
                Document.class
        );
        List<String> distinctPodList = distinctPod
                .getMappedResults()
                .stream()
                .map(doc -> doc.getString("_id"))
                .collect(Collectors.toList());

        AggregationOperation valDataSort = sort(Sort.by("MEAS_YMDD_ID", "dataFilenameMagnitude", "POD", "MEAS_TYPE"));

        Aggregation valDataAggregation = newAggregation(project, match, valDataSort);
        AggregationResults<Document> valDataAggregationResult = mongoTemplate.aggregate(
                valDataAggregation,
                "curveQuartorarie",
                Document.class
        );

        HashMap<String, Double> podValuesList = new HashMap<>();
        for  (Document doc : valDataAggregationResult.getMappedResults()){
            podValuesList.put(doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD")+ "_" + doc.getString("MEAS_TYPE"), doc.getDouble("val"));
        }


        System.out.println("Numero di record nella mappa: " + podValuesList.size());
    }
}
