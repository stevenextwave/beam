package org.apache.beam.examples.test;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Instant;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Helper {

    public ManagedChannel channel;
    public TransportChannelProvider transportChannelProvider;
    public TopicAdminClient topicAdminClient;
    public SubscriptionAdminClient subscriptionAdminClient;
    public CredentialsProvider credentialsProvider;
    public String host, projectId;

    public Helper(String host, String projectId) {
        this.host = host;
        this.projectId = projectId;
        channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
        transportChannelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        credentialsProvider = NoCredentialsProvider.create();
        try {
            topicAdminClient = createTopicAdmin(credentialsProvider);
            subscriptionAdminClient = createSubscriberAdmin(credentialsProvider);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private TopicAdminClient createTopicAdmin(CredentialsProvider provider) throws IOException {
        return TopicAdminClient.create(TopicAdminSettings.newBuilder().setTransportChannelProvider(transportChannelProvider)
                .setCredentialsProvider(credentialsProvider).build());
    }

    private SubscriptionAdminClient createSubscriberAdmin(CredentialsProvider provider) throws IOException {
        return SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder().build().newBuilder().setTransportChannelProvider(transportChannelProvider)
                .setCredentialsProvider(credentialsProvider).build());
    }

    public void deleteTopic(String topicName) {
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        topicAdminClient.deleteTopic(projectTopicName);
    }

    public void deleteSubscription(String subscriptionName) {
        ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);
        subscriptionAdminClient.deleteSubscription(projectSubscriptionName);
    }

    public Topic createTopic(String topicName) {
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        return topicAdminClient.createTopic(projectTopicName);
    }

    public Subscription createSubscription(String topicName, String subscriptionName) {
        ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        return subscriptionAdminClient.createSubscription(projectSubscriptionName, projectTopicName, PushConfig.getDefaultInstance(), 0);
    }

    public void sendMessage(String topicName, String message) {
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(projectTopicName).setChannelProvider(transportChannelProvider).setCredentialsProvider(credentialsProvider).build();
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data).putAttributes("timestamp", Long.toString(Instant.now().getMillis())).build();
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
                @Override
                public void onFailure(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onSuccess(String s) {
                    log.info("Sending Id =" + s);

                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (publisher != null) {
                publisher.shutdown();
                try {
                    publisher.awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
