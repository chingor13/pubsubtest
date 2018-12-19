package com.google.example;

import com.google.cloud.pubsub.v1.Subscriber;

/**
 * Hello world!
 *
 */
public class App 
{
  public static final String PROJECT_ID = "chingor-php-gcs";
  public static final String TOPIC_NAME = "test-topic";
  public static final String SUBSCRIPTION_NAME = "test-subscription";

  public static void main( String[] args ) throws Exception {
    System.out.println( "Hello World!" );

    System.out.println("Starting publisher");
    // new PublisherTest(PROJECT_ID, TOPIC_NAME).run();
    System.out.println("Starting subscriber");
    Subscriber subscriber = new SubscriberTest(PROJECT_ID, SUBSCRIPTION_NAME, TOPIC_NAME).run();

    Thread.sleep(Long.MAX_VALUE);

    subscriber.stopAsync().awaitTerminated();
  }
}
