package com.pipeline.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pipeline.model.AddToCartEvent;
import com.pipeline.model.ClickEvent;
import com.pipeline.model.ProductViewEvent;
import com.pipeline.model.PurchaseEvent;
import com.pipeline.model.RemoveFromCartEvent;
import com.pipeline.model.SearchEvent;
import com.pipeline.serialization.JsonMapperFactory;

public class EventPersister {
    private static final Logger log = LoggerFactory.getLogger(EventPersister.class);
    private static final ObjectMapper mapper = JsonMapperFactory.createMapper();

    public static String describe(ClickEvent event) {
        return switch (event) {
            case ProductViewEvent e -> "Product viewed: " + e;
            case AddToCartEvent e -> "Added to cart: " + e;
            case RemoveFromCartEvent e -> "Removed from cart: " + e;
            case SearchEvent e -> "Search performed: " + e;
            case PurchaseEvent e -> "Purchase completed: " + e;
        };
    }

    private static void writeToMongo(ClickEvent event, MongoCollection<Document> collection) {
        try {
            String json = mapper.writeValueAsString(event);
            Document doc = Document.parse(json);
            collection.insertOne(doc);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize event for Mongo write", e);
        } catch (MongoException e) {
            log.error("Failed to write event to MongoDB", e);
        }
    }

    public static void handleProductView(ProductViewEvent event, MongoCollection<Document> collection) {
        writeToMongo(event, collection);
    }

    public static void handleAddToCart(AddToCartEvent event, MongoCollection<Document> collection) {
        writeToMongo(event, collection);
    }

    public static void handleRemoveFromCart(RemoveFromCartEvent event, MongoCollection<Document> collection) {
        writeToMongo(event, collection);
    }

    public static void handleSearch(SearchEvent event, MongoCollection<Document> collection) {
        writeToMongo(event, collection);
    }

    public static void handlePurchase(PurchaseEvent event, MongoCollection<Document> collection) {
        writeToMongo(event, collection);
    }
}