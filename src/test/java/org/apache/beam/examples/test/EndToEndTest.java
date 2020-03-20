package org.apache.beam.examples.test;


import com.google.gson.Gson;
import com.google.rpc.Help;
import junit.textui.TestRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.examples.PubSubExecutor;
import org.apache.beam.examples.Result;
import org.apache.beam.examples.TestOptions;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(JUnit4ClassRunner.class)
@Slf4j
public class EndToEndTest {

    private String host = "localhost:8085";
    private String projectId = "test-proj";
    private Gson gson = new Gson();

    private Helper helper = new Helper(host,projectId);
    private TestOptions testOptions = PipelineOptionsFactory.as(TestOptions.class);

    @Before
    public void setup() throws Exception
    {
        testOptions.setPubsubRootUrl("http://localhost:8085");
        testOptions.setCredentialFactoryClass(NoopCredentialFactory.class);
        testOptions.setBlockOnRun(false);
        helper.createTopic("test");
        helper.createSubscription("test","testsub");
        Result result = new Result(UUID.randomUUID().toString(),"testmesssge");
        helper.sendMessage("test",gson.toJson(result));
    }

    @After
    public void after() throws  Exception
    {
        helper.deleteSubscription("testsub");
        helper.deleteTopic("test");
    }

    @Test
    public void doRun()
    {
        PubSubExecutor pubSubExecutor = new PubSubExecutor();
        pubSubExecutor.run(testOptions);
    }

}
