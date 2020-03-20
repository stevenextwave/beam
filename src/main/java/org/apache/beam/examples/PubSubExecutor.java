package org.apache.beam.examples;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PubSubExecutor {

    private Gson gson = new Gson();

    public PubsubMessage createMessage() {
        Result result = new Result(UUID.randomUUID().toString(), "testmesssge");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("timestamp", Long.toString(Instant.now().getMillis()));
        return new PubsubMessage(gson.toJson(result).getBytes(), attributes);
    }

    public void run(TestOptions options) {
        Pipeline p = Pipeline.create(options);
        PCollection<Result> result = p.apply("pubsub",
                PubsubIO.readMessagesWithAttributes().fromSubscription("projects/test-proj/subscriptions/testsub"))
                .apply("window",Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("convert pubsub", ParDo.of(new PubSubToResult()));

        /*PCollection<Result> result = p.apply(Create.of(createMessage()))
                .apply("convert pubsub", ParDo.of(new PubSubToResult()));*/

        PCollection<KV<String, Result>> sorted = result.apply("index", WithKeys.<String, Result>of(a -> a.getId()).withKeyType(TypeDescriptors.strings()));
        sorted.apply(ParDo.of(new SortedByKey())).apply(TextIO.write().to("result.txt").withWindowedWrites().withNumShards(1));
        p.run().waitUntilFinish(Duration.standardSeconds(30));
    }
}
