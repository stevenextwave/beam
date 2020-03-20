package org.apache.beam.examples;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;

public interface TestOptions extends PubsubOptions, DirectOptions {
}
