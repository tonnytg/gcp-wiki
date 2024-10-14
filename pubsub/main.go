// This code create a new topic and subscription in Google Cloud Pub/Sub
// Replace "your-project-id" with your Google Cloud project ID
// Replace "my-topic" with the desired topic ID
// Replace "my-subscription" with the desired subscription ID

package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

func main() {
	// Set up context and Google Cloud project ID
	ctx := context.Background()
	projectID := "your-project-id"
	topicID := "my-topic"
	subscriptionID := "my-subscription"

	// Create a Pub/Sub client
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	// Create a topic
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("Topic created: %v\n", topic)

	// Create a subscription for the topic
	sub, err := client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10, // set acknowledgment deadline (in seconds)
	})
	if err != nil {
		log.Fatalf("Failed to create subscription: %v", err)
	}
	fmt.Printf("Subscription created: %v\n", sub)
}
