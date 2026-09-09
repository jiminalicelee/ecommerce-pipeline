package com.pipeline.producer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.pipeline.model.AddToCartEvent;
import com.pipeline.model.ClickEvent;
import com.pipeline.model.OrderLineItem;
import com.pipeline.model.ProductViewEvent;
import com.pipeline.model.PurchaseEvent;
import com.pipeline.model.RemoveFromCartEvent;
import com.pipeline.model.SearchEvent;

public class FakeEventGenerator {
    private static final List<String> CATEGORIES = List.of(
            "Electronics", "Books", "Clothing", "Home & Garden",
            "Sports & Outdoors", "Toys & Games", "Beauty", "Grocery");

    public static ClickEvent generateRandomEvent(Random random) {
        int eventType = random.nextInt(5);
        return switch (eventType) {
            case 0 -> generateProductView(random);
            case 1 -> generateAddToCart(random);
            case 2 -> generateRemoveFromCart(random);
            case 3 -> generateSearch(random);
            case 4 -> generatePurchase(random);
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    private static ClickEvent generateProductView(Random random) {
        return new ProductViewEvent(
                UUID.randomUUID().toString(), // eventId
                UUID.randomUUID().toString(), // anonymousId
                null, // userId
                UUID.randomUUID().toString(), // sessionId
                Instant.now(), // eventTimestamp
                UUID.randomUUID().toString(), // productId
                CATEGORIES.get(random.nextInt(CATEGORIES.size())),
                generateRandomPrice(random));
    }

    private static ClickEvent generateAddToCart(Random random) {
        return new AddToCartEvent(
                UUID.randomUUID().toString(), // eventId
                UUID.randomUUID().toString(), // anonymousId
                null, // userId
                UUID.randomUUID().toString(), // sessionId
                Instant.now(), // eventTimestamp
                UUID.randomUUID().toString(), // productId
                generateRandomPrice(random),
                random.nextInt(1, 6));
    }

    private static ClickEvent generateRemoveFromCart(Random random) {
        return new RemoveFromCartEvent(
                UUID.randomUUID().toString(), // eventId
                UUID.randomUUID().toString(), // anonymousId
                null, // userId
                UUID.randomUUID().toString(), // sessionId
                Instant.now(), // eventTimestamp
                UUID.randomUUID().toString(), // productId
                generateRandomPrice(random),
                random.nextInt(1, 3));
    }

    private static ClickEvent generateSearch(Random random) {
        return new SearchEvent(
                UUID.randomUUID().toString(), // eventId
                UUID.randomUUID().toString(), // anonymousId
                null, // userId
                UUID.randomUUID().toString(), // sessionId
                Instant.now(), // eventTimestamp
                UUID.randomUUID().toString(), // searchQuery
                random.nextInt(21));
    }

    private static ClickEvent generatePurchase(Random random) {
        return new PurchaseEvent(
                UUID.randomUUID().toString(), // eventId
                UUID.randomUUID().toString(), // anonymousId
                null, // userId
                UUID.randomUUID().toString(), // sessionId
                Instant.now(), // eventTimestamp
                generateLineItems(random));
    }

    private static double generateRandomPrice(Random random) {
        double rawPrice = 50.0 + (random.nextDouble() * 495.0); // $50.00–$500.00
        return Math.round(rawPrice * 100.0) / 100.0;
    }

    private static List<OrderLineItem> generateLineItems(Random random) {
        List<OrderLineItem> lineItems = new ArrayList<>();
        for (int i = 0; i < random.nextInt(1, 11); i++) {
            lineItems.add(new OrderLineItem(
                    UUID.randomUUID().toString(),
                    generateRandomPrice(random),
                    random.nextInt(1, 4)));
        }
        return lineItems;
    }
}