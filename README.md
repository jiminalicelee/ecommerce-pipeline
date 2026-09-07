# Real-time e-commerce clickstream pipeline

A small event-driven data pipeline built in Java, demonstrating Kafka
and MongoDB working together: a producer simulates e-commerce
clickstream events (views, cart actions, purchases, searches),
publishes them to Kafka, and a consumer processes them into MongoDB.