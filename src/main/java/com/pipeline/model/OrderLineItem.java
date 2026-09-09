package com.pipeline.model;

/**
 * A single line item within a purchase: one product, its quantity,
 * and the price at time of purchase.
 * OrderLineItem
 * @param productId
 * @param productPrice
 * @param quantity
 */
public record OrderLineItem(
        String productId,
        double productPrice,
        int quantity
) {
    public double lineItemTotal() {
        return productPrice * quantity;
    }
}