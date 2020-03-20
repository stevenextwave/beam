package org.apache.beam.examples;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class PubSubToResult extends DoFn<PubsubMessage, Result> {
    private static Gson gson;

    @StartBundle
    public void setup() {
        gson = new Gson();
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<Result> output) {
        log.info("recv="+new String(pubsubMessage.getPayload()));
        output.output(gson.fromJson(new String(pubsubMessage.getPayload()), Result.class));
    }
}
