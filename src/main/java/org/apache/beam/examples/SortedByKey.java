package org.apache.beam.examples;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;

@RequiredArgsConstructor
@Slf4j
public class SortedByKey extends DoFn<KV<String,Result>,String>
{
    @TimerId("timer")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(BoundedWindow window, @Element KV<String,Result> element,
                               OutputReceiver<String> output, @TimerId("timer") Timer expiryTimer){
        log.info("set timer" + window.maxTimestamp());
        expiryTimer.set(window.maxTimestamp());
    }

    @OnTimer("timer")
    public void onExpiry(OnTimerContext timerContext)
    {
        log.info("expired");
        timerContext.output("Done");
    }






}
