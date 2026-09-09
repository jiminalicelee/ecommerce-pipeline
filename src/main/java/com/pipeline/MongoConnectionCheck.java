package com.pipeline;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoConnectionCheck {
    private static final String CONNECTION_STRING = "mongodb://localhost:27017";
    private static final Logger log = LoggerFactory.getLogger(MongoConnectionCheck.class);

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            MongoDatabase database = mongoClient.getDatabase("clickstream");
            MongoCollection<Document> collection = database.getCollection("test");

            Document doc = new Document("message", "hello from Java driver")
                    .append("timestamp", System.currentTimeMillis());

            collection.insertOne(doc);
            log.info("Inserted document with _id: " + doc.getObjectId("_id"));

            // Read it back to confirm the write actually landed
            Document found = collection.find().first();
            log.info("Retrieved document: " + found.toJson());
        }
    }
}