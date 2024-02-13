package com.dxc.quartorariecsvgenerator;

import org.apache.commons.lang3.StringUtils;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
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
        for (Document doc : valDataAggregationResult.getMappedResults()) {
            podValuesList.put(doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD") + "_" + doc.getString("MEAS_TYPE"), doc.getDouble("val"));
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
        SimpleDateFormat completeDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat cycleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date startDateTime = dateFormat.parse(startDate);
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(startDateTime);
        Date endDateTime = dateFormat.parse(endDate);
        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(endDateTime);

        List<String> meastypeList = Arrays.asList("0", "1", "2");

        String csvFileName = fileName + "_" + magnitude + ".csv";
        File file = new File(csvFileName);
        if (file.exists()) {
            file.delete();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFileName))) {

            String header = "MEASYM_MEASDD_MEASTYPE;";
            for (String runningPod : distinctPodList){
                header += runningPod + ";";
            }
            writer.write(header + "\n");

            // Cycle on days
            String line = "";
            for (Calendar runningCal = startCalendar; runningCal.before(endCalendar); runningCal.add(Calendar.DAY_OF_YEAR, 1)) {
                for (int runningId = 1; runningId <= 96; runningId++) {
                    line = completeDateFormat.format(runningCal.getTime());
                    line += " ";
                    line += idToString(runningId);
                    line += ";";
                    for (String runningPod : distinctPodList) {
                        String values = "";
                        for (String runningMeastype : meastypeList) {
                            Double val = podValuesList.get(
                                    cycleDateFormat.format(runningCal.getTime())
                                            + "_"
                                            + StringUtils.leftPad(String.valueOf(runningId), 2, "0")
                                            + "_"
                                            + runningPod + "_" + runningMeastype
                            );
                            if (val != null) {
                                values += val + "_" + runningMeastype;
                            }
                        }

                        line += values + ";";

                    }
                    writer.write(line + "\n");
                }
            }
        }
        System.out.println("Numero di record nella mappa: " + podValuesList.size());
    }

    private String idToString(int number) {
        int hours = (number - 1) / 4;
        int minutes = (number - 1) % 4 * 15;

        // Formattiamo le ore e i minuti in una stringa con due cifre
        String formattedHours = String.format("%02d", hours);
        String formattedMinutes = String.format("%02d", minutes);

        return formattedHours + ":" + formattedMinutes;

    }
}
