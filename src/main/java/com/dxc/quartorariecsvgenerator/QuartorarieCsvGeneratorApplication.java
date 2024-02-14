package com.dxc.quartorariecsvgenerator;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.exit;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@SpringBootApplication
public class QuartorarieCsvGeneratorApplication implements CommandLineRunner {

    private static final Logger logger = LogManager.getLogger(QuartorarieCsvGeneratorApplication.class);

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

        logger.debug("Args received");

        // Setup file
        String csvFileName = fileName + "_" + magnitude + ".csv";
        File file = new File(csvFileName);
        if (file.exists()) {
            file.delete();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFileName))) {

            // Get distinct PODs list
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
            logger.debug("List of distinct PODS has " + distinctPodList.size() + " elements");

            // Write csv file header
            String header = "MEASYM_MEASDD_MEASTYPE;";
            for (String runningPod : distinctPodList) {
                header += runningPod + ";";
            }
            writer.write(header + "\n");

            // Setup days for first execution
            SimpleDateFormat datesFormat = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(datesFormat.parse(startDate));
            String minDay = datesFormat.format(calendar.getTime());
            calendar.add(Calendar.DAY_OF_YEAR, 1);
            String maxDay = datesFormat.format(calendar.getTime());
            logger.debug("minDay = " + minDay + ", " + "maxDay = " + maxDay);

            // Looping through days
            HashMap<String, String> podValuesList = new HashMap<>();
            while (StringUtils.compare(maxDay, endDate) <= 0) {
                podValuesList.clear();
                AggregationOperation timeSubsetMatch = match(
                        where("MEAS_YMDD_ID").gte(minDay).lt(maxDay)
                                .and("dataFilenameMagnitude").is(fileName + "_" + magnitude)
                                .and("POD").in(distinctPodList)
                );
                AggregationOperation valDataSort = sort(Sort.by("MEAS_YMDD_ID", "dataFilenameMagnitude", "POD", "MEAS_TYPE"));
                AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
                Aggregation valDataAggregation = newAggregation(project, timeSubsetMatch, valDataSort).withOptions(options);
                AggregationResults<Document> valDataAggregationResult = mongoTemplate.aggregate(
                        valDataAggregation,
                        "curveQuartorarie",
                        Document.class
                );
                for (Document doc : valDataAggregationResult.getMappedResults()) {
                    podValuesList.put(doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD"), doc.getDouble("val") + "_" + doc.getString("MEAS_TYPE"));
                }

                String csvLine = "";
                for (int runningId = 1; runningId <= 96; runningId++) {
//                    logger.debug("runningId = " + runningId);
                    csvLine += sdfOutput.format(datesFormat.parse(minDay));
                    csvLine += " ";
                    csvLine += idToString(runningId);
                    csvLine += ";";
                    for (String runningPod : distinctPodList) {
                        String values = podValuesList.get(minDay
                                + "_"
                                + StringUtils.leftPad(String.valueOf(runningId), 2, "0")
                                + "_"
                                + runningPod
                        );

                        csvLine += (values != null ? values : "") + ";";

                    }
                    writer.write(csvLine + "\n");
                    csvLine = "";
//                    logger.debug("Write to csv: " + csvLine);
                }
                minDay = maxDay;
                calendar.setTime(datesFormat.parse(minDay));
                calendar.add(Calendar.DAY_OF_YEAR, 1);
                maxDay = datesFormat.format(calendar.getTime());
                logger.debug("minDay = " + minDay + ", " + "maxDay = " + maxDay);
            }
        }
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
