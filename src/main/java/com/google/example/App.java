package com.google.example;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );


        new SubscriberTest("chingor-php-gcs", "test-subscription", "test-topic").run();
    }

    static class MessageReceiverExample implements MessageReceiver {

        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            System.out.println(message);
            System.out.println(consumer);
            consumer.ack();
        }
    }

    static class SubscriberTest {
        private String projectId;
        private String subscriptionName;
        private String topicName;

        public SubscriberTest(String projectId, String subscriptionName, String topicName) {
            this.projectId = projectId;
            this.subscriptionName = subscriptionName;
            this.topicName = topicName;
        }

        private void ensureSubscriptionExists(ProjectSubscriptionName projectSubscriptionName) {
            try (SubscriptionAdminClient adminClient = SubscriptionAdminClient.create()) {
                try {
                    Subscription subscription = adminClient
                        .getSubscription(projectSubscriptionName);
                } catch (NotFoundException e) {
                    // failed to find resource, create it
                    ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
                    PushConfig pushConfig = PushConfig.newBuilder().build();
                    adminClient.createSubscription(projectSubscriptionName, projectTopicName, pushConfig, 30);
                }
            } catch (IOException e) {
                System.out.println(e);
            }

        }

        public void run() {

            ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);
            ensureSubscriptionExists(projectSubscriptionName);
            Subscriber subscriber = null;

            try {
                subscriber = Subscriber
                    .newBuilder(projectSubscriptionName, new MessageReceiverExample()).build();
                subscriber.startAsync().awaitRunning();
                for (; ; ) {
                    Thread.sleep(Long.MAX_VALUE);
                }
            } catch (InterruptedException e) {
                // do nothing
            } finally {
                if (subscriber != null) {
                    subscriber.stopAsync();
                }
            }
        }
    }
}
