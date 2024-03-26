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
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("QUARTORARIE")) {
                logger.debug(key + "=" + value);
            }
        });
        SpringApplication.run(QuartorarieCsvGeneratorApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        long fileStartTimeMillis = System.currentTimeMillis();
        if (args.length != 4 && args.length !=10) {
            System.err.println("Usage (1): java -jar QuartorarieCsvGeneratorApplication.jar magnitude fileName startDate(yyyyMMdd) endDate(yyyyMMdd)");
            System.err.println("Usage (2): java -jar QuartorarieCsvGeneratorApplication.jar magnitude fileName startDate(yyyyMMdd) endDate(yyyyMMdd) username password mongoHost mongoPort dbName recreateIndex(true or false)");
            exit(1);
        }


        String magnitude = args[0];
        String fileName = args[1];
        String startDate = args[2];
        String endDate = args[3];

        if (args.length == 10){
            mongoTemplate = new MongoTemplate(
                    new SimpleMongoClientDatabaseFactory(
                            "mongodb://" + args[4] + ":" + args[5] + "@" + args[6] + ":" + args[7] + "/" + args[8]
                    )
            );
        }

        String collectionName = fileName + "_" + magnitude;

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date pStartDate = sdf.parse(startDate);
            Date pEndDate = sdf.parse(endDate);
        } catch (ParseException e) {
            System.err.println("startDate / endDate must be in \"yyyyMMdd\" format");
            exit(2);
        }

        logger.debug("Args received");

        // Setup file
        String csvFileName = fileName + "_" + magnitude + ".csv";
        File file = new File(csvFileName);
        if (file.exists()) {
            file.delete();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFileName))) {
            if (args[9].equalsIgnoreCase("true"))
            {
                if (mongoTemplate.indexOps(fileName + "_" + magnitude).getIndexInfo().stream()
                        .anyMatch(indexInfo -> indexInfo.getName().equals(fileName + "_" + magnitude + "_java"))) {
                    mongoTemplate.indexOps(fileName + "_" + magnitude).dropIndex(fileName + "_" + magnitude + "_java");
                }
                logger.debug("Creating index " + fileName + "_" + magnitude + "_java");
                mongoTemplate.indexOps(fileName + "_" + magnitude)
                        .ensureIndex(
                                new Index()
                                        .on("POD", Sort.Direction.ASC)
                                        .on("MEAS_YMDD_ID", Sort.Direction.ASC)
                                        .named(fileName + "_" + magnitude + "_java")
                        );
                logger.info("Index " + fileName + "_" + magnitude + " created");
            }

            // Get distinct PODs list
            AggregationOperation distinctPodGroupByPod = group("POD");
            AggregationOperation distinctPodSortByPod = sort(Sort.by("_id"));
            Aggregation distinctPodAggregation = Aggregation.newAggregation(distinctPodGroupByPod, distinctPodSortByPod);
            AggregationResults<Document> distinctPod = mongoTemplate.aggregate(distinctPodAggregation, collectionName, Document.class);
            List<String> distinctPodList = distinctPod.getMappedResults().stream().map(doc -> doc.getString("_id")).collect(Collectors.toList());
            logger.info("List of distinct PODS has " + distinctPodList.size() + " elements");
            if (distinctPodList.size() == 0) {
                System.err.println("db returned no PODs to perform this operation");
                exit(5);
            }

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

            Query countQuery = new Query(where("MEAS_YMDD_ID").gte(startDate).lt(endDate).and("POD").in(distinctPodList));
            AggregationOperation countFirstMatchOperation = match(where("MEAS_YMDD_ID").gte(startDate).lt(endDate).and("POD").in(distinctPodList));
            AggregationOperation limitOperation = limit(1);
            AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
            Aggregation countAggregation = newAggregation(countFirstMatchOperation, limitOperation).withOptions(options);
            AggregationResults<Document> countAggregationResult = mongoTemplate.aggregate(countAggregation, collectionName, Document.class);

            long countRecords = countAggregationResult.getMappedResults().size();

            if (countRecords == 0) {
                System.err.println("db returned no measurements to perform this operation");
                exit(5);
            }
            // Looping through days

            HashMap<String, String> podValuesHashMap = new HashMap<>();
            HashSet<String> podsWithSomeNullValues = new HashSet<>();
            while (StringUtils.compare(maxDay, endDate) <= 0) {
                long startTimeMillis = System.currentTimeMillis();
                podValuesHashMap.clear();
                logger.debug("Querying data from minDay = " + minDay + " to maxDay = " + maxDay);

                AggregationOperation timeSubsetMatch = match(where("MEAS_YMDD_ID").gte(minDay).lt(maxDay).and("POD").in(distinctPodList));
                AggregationOperation valDataSort = sort(Sort.by("MEAS_YMDD_ID", "POD", "MEAS_TYPE"));
                AggregationOperation project = project()
                        .and("MEAS_YMDD_ID").as("MEAS_YMDD_ID")
                        .and("POD").as("POD")
                        .and("MEAS_TYPE").as("MEAS_TYPE")
                        .and("val").as("val")
                        .and("id").as("id")
                        .andExclude("_id");
                AggregationOptions countOptions = AggregationOptions.builder().allowDiskUse(true).build();
                Aggregation valDataAggregation = newAggregation(timeSubsetMatch, project, valDataSort).withOptions(countOptions);
                AggregationResults<Document> valDataAggregationResult = mongoTemplate.aggregate(valDataAggregation, collectionName, Document.class);

                /*
                long startTimeMillis = System.currentTimeMillis();
                Iterator<Document> it = valDataAggregationResult.getMappedResults().iterator();
                Document iteratingDoc = it.next();
                for (int runningId = 1; runningId <= 96; runningId++) {
                    String csvLine = "";
                    csvLine += sdfOutput.format(datesFormat.parse(minDay)) + " "+ idToString(runningId)+ ";";
                    for (String distinctSortedPod : distinctPodList) {
                        if (iteratingDoc.getString("POD").equalsIgnoreCase(distinctSortedPod) && iteratingDoc.getString("id").equals(String.format("%02d", runningId))){
                            csvLine += iteratingDoc.getDouble("val") + "_" + iteratingDoc.getString("MEAS_TYPE") + ";";
                            try {
                                iteratingDoc = it.next();
                            }
                            catch (NoSuchElementException e ){
                                iteratingDoc = new Document().append("POD", "").append("val", null);
                            }
                        }
                        else {
                            csvLine += ";";
                        }
                    }

                    logger.debug(csvLine);
                    writer.write(csvLine + "\n");
                }
                logger.debug("Iterator cycle completed in " + (System.currentTimeMillis() - startTimeMillis) + " milliseconds");
                */

                String currVal = "";
                for (Document doc : valDataAggregationResult.getMappedResults()) {
                    currVal = podValuesHashMap.get(doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD"));
                    if (currVal == null || currVal.startsWith("0.0_"))
                        podValuesHashMap.put(doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD"), doc.getDouble("val") + "_" + doc.getString("MEAS_TYPE"));
                    else
                        logger.warn("podValuesHashMap already contains value = " + currVal + " for key = " + doc.getString("MEAS_YMDD_ID") + "_" + doc.getString("POD"));
                }

                String csvLine = "";
                for (int runningId = 1; runningId <= 96; runningId++) {
                    csvLine += sdfOutput.format(datesFormat.parse(minDay));
                    csvLine += " ";
                    csvLine += idToString(runningId);
                    csvLine += ";";
                    for (String runningPod : distinctPodList) {
                        String values = podValuesHashMap.get(minDay + "_" + StringUtils.leftPad(String.valueOf(runningId), 2, "0") + "_" + runningPod);

                        csvLine += (values != null ? values : "") + ";";
                        if (values == null)
                            podsWithSomeNullValues.add(runningPod);

                    }
                    logger.trace(csvLine);
                    writer.write(csvLine + "\n");
                    csvLine = "";
                }
                logger.info("Written file " + fileName + "_" + magnitude + ".csv data from minDay = " + minDay + " to maxDay = " + maxDay + " in " + (System.currentTimeMillis() - startTimeMillis) + " milliseconds");

                minDay = maxDay;
                calendar.setTime(datesFormat.parse(minDay));
                calendar.add(Calendar.DAY_OF_YEAR, 1);
                maxDay = datesFormat.format(calendar.getTime());
            }
            mongoTemplate.indexOps(fileName + "_" + magnitude).dropIndex(fileName + "_" + magnitude + "_java");
            logger.info("podsWithSomeNullValues contains " + podsWithSomeNullValues.size() + " elements:");
            podsWithSomeNullValues.forEach(logger::info);
        }
        logger.debug("File " + fileName + " written in " + (System.currentTimeMillis() - fileStartTimeMillis) + "ms");
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
