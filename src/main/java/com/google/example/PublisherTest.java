package com.google.example;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PublisherTest {
  private String projectId;
  private String topicName;

  public PublisherTest(String projectId, String topicName) {
    this.projectId = projectId;
    this.topicName = topicName;
  }


  private void ensureTopicExists(ProjectTopicName projectTopicName) {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      try {
        Topic topic = topicAdminClient.getTopic(projectTopicName);
      } catch (NotFoundException e) {
        topicAdminClient.createTopic(projectTopicName);
      }
    } catch (IOException e) {
      System.out.println(e);
    }

  }

  public void run() throws Exception {
    ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
    // ensureTopicExists(projectTopicName);

    Publisher publisher = null;
    List<ApiFuture<String>> futures = new ArrayList<>();
    try {
      publisher = Publisher.newBuilder(projectTopicName).build();

      for (int i = 0; i < 5; i++) {
        String message = "message-" + i;

        // convert message to bytes
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(data)
            .build();

        ApiFuture<String> future = publisher.publish(pubsubMessage);
        futures.add(future);
      }
    } finally {
      List<String> messageIds = ApiFutures.allAsList(futures).get();

      for (String messageId : messageIds) {
        System.out.println(messageId);
      }

      if (publisher != null) {
        publisher.shutdown();
      }
    }
  }
}
