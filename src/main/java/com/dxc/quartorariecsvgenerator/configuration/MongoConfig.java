package com.dxc.quartorariecsvgenerator.configuration;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoConfig extends AbstractMongoClientConfiguration {

    @Value("a2a")
    private String database;

    @Value("${QUARTORARIE_MONGODB_HOST}")
    private String host;

    @Value("${QUARTORARIE_MONGODB_PORT}")
    private String port;

    @Value("${QUARTORARIE_MONGODB_DBNAME}")
    private String dbname;

    @Value("${QUARTORARIE_MONGODB_USERNAME}")
    private String username;

    @Value("${QUARTORARIE_MONGODB_PASSWORD}")
    private String password;

    @Override
    protected String getDatabaseName() {
        return database;
    }

    @Override
    public MongoClient mongoClient() {
        ConnectionString connectionString = new ConnectionString("mongodb://" + username + ":" + password + "@" + host + ":" + port + "/" + dbname);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        return MongoClients.create(mongoClientSettings);
    }
}
