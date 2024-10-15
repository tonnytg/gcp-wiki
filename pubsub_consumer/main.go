package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gorilla/mux"
)

// Message represents the structure of the message to be published
type Message struct {
	Data string `json:"data"`
}

// Global variables for Pub/Sub client, topic, and subscription
var (
	client         *pubsub.Client
	topic          *pubsub.Topic
	subscription   *pubsub.Subscription
	projectID      = "your-project-id"
	topicID        = "my-topic"
	subscriptionID = "my-subscription"
)

func initPubSub(ctx context.Context) {
	var err error
	// Create a Pub/Sub client
	client, err = pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}

	// Initialize topic
	topic = client.Topic(topicID)
	// Check if topic exists, if not, create it
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatalf("Failed to check if topic exists: %v", err)
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			log.Fatalf("Failed to create topic: %v", err)
		}
	}

	// Initialize subscription
	subscription = client.Subscription(subscriptionID)
	exists, err = subscription.Exists(ctx)
	if err != nil {
		log.Fatalf("Failed to check if subscription exists: %v", err)
	}
	if !exists {
		// Create subscription if it does not exist
		subscription, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			log.Fatalf("Failed to create subscription: %v", err)
		}
	}
}

// HTTP handler to receive messages and publish them to the topic
func publishMessage(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var msg Message

	// Parse the incoming JSON request
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Publish the message to Pub/Sub
	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(msg.Data),
	})

	// Check for publish errors
	id, err := result.Get(ctx)
	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		return
	}

	log.Printf("Published message with ID: %v", id)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Message published successfully: %v", id)
}

// Function to consume messages from the subscription
func consumeMessages() {
	ctx := context.Background()
	log.Println("Starting message consumer...")

	// Pull messages from the subscription
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Printf("Received message: %s", string(msg.Data))
		// Acknowledge the message
		msg.Ack()
	})

	if err != nil {
		log.Fatalf("Failed to consume messages: %v", err)
	}
}

func main() {
	ctx := context.Background()

	// Initialize Pub/Sub
	initPubSub(ctx)

	// Create a new HTTP router
	r := mux.NewRouter()

	// Define the POST endpoint
	r.HandleFunc("/publish", publishMessage).Methods("POST")

	// Start the HTTP server in a separate goroutine
	go func() {
		log.Println("Starting HTTP server on port 8080")
		if err := http.ListenAndServe(":8080", r); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Start the message consumer
	consumeMessages()
}
