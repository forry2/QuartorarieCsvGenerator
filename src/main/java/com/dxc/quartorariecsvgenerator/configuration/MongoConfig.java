package com.dxc.quartorariecsvgenerator.configuration;

import com.dxc.quartorariecsvgenerator.QuartorarieCsvGeneratorApplication;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoConfig extends AbstractMongoClientConfiguration {
    private static final Logger logger = LogManager.getLogger(MongoConfig.class);

    @Value("${spring.data.mongodb.database}")
    private String database;

    @Value("${QUARTORARIE_MONGODB_HOST:awpral007.group.local}")
    private String host;

    @Value("${spring.data.mongodb.port}")
    private String port;

    @Value("${spring.data.mongodb.database}")
    private String dbname;

    @Value("${spring.data.mongodb.username}")
    private String username;

    @Value("${spring.data.mongodb.password}")
    private String password;

    @Override
    protected String getDatabaseName() {
        return database;
    }

    @Override
    public MongoClient mongoClient() {
//        logger.debug("mongodb://" + username + ":" + password + "@" + host + ":" + port + "/" + dbname);
        ConnectionString connectionString = new ConnectionString("mongodb://" + username + ":" + password + "@" + host + ":" + port + "/" + dbname);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        return MongoClients.create(mongoClientSettings);
    }
}
