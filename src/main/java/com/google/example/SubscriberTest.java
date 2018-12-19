package com.google.example;

import com.google.api.core.ApiService.State;
import com.google.api.gax.grpc.GrpcInterceptorProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SubscriberTest {
  private String projectId;
  private String subscriptionName;
  private String topicName;

  public SubscriberTest(String projectId, String subscriptionName, String topicName) {
    this.projectId = projectId;
    this.subscriptionName = subscriptionName;
    this.topicName = topicName;
  }

  static class MessageReceiverExample implements MessageReceiver {

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      System.out.println(message);
      // System.out.println(consumer);
      consumer.ack();
    }
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

  public Subscriber run() {

    ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);
    // ensureSubscriptionExists(projectSubscriptionName);
    Subscriber subscriber = null;
    final ClientInterceptor interceptor = new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        System.out.println("intercepting call");
        System.out.println(method);
        System.out.println(callOptions);
        if (method.getFullMethodName().endsWith("StreamingPull")) {
          System.out.println("!!!!!!!!!!!!!!!!!");
        }
        return next.newCall(method, callOptions);
        // return null;
      }
    };
    GrpcInterceptorProvider interceptorProvider = new GrpcInterceptorProvider() {
      @Override
      public List<ClientInterceptor> getInterceptors() {
        return Arrays.asList(interceptor);
      }
    };
    TransportChannelProvider channelProvider =
        SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
            .setInterceptorProvider(interceptorProvider)
            .build();

    subscriber = Subscriber
        .newBuilder(projectSubscriptionName, new MessageReceiverExample())
        .setChannelProvider(channelProvider)
        .build();
    subscriber.addListener(new Subscriber.Listener() {
      @Override
      public void failed(State from, Throwable failure) {
        System.out.println("Subscriber failed");
        System.out.println(from);
        System.out.println(failure);
        // System.out.println(failure.getStackTrace());
        super.failed(from, failure);
      }

      @Override
      public void running() {
        System.out.println("running");
        super.running();
      }

      @Override
      public void starting() {
        System.out.println("starting");
        super.starting();
      }

      @Override
      public void stopping(State from) {
        System.out.println("stopping");
        super.stopping(from);
      }

      @Override
      public void terminated(State from) {
        System.out.println("terminated!!!!!!!!!!!!!!");
        super.terminated(from);
      }
    }, MoreExecutors.directExecutor());
    subscriber.startAsync().awaitRunning();

    return subscriber;
  }
}
