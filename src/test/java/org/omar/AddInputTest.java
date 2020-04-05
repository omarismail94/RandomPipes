package org.omar;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;


@RunWith(JUnit4.class)
public class AddInputTest {



    @Rule public final transient TestPipeline pipeline = TestPipeline.create();


    @Test
    @Category(NeedsRunner.class)
    public void testAddInput() throws Exception {
        // Create the test input.
        AddToInput.MyOptions options = TestPipeline.testingPipelineOptions().as(AddToInput.MyOptions.class);

        final List<Integer> LINES = Arrays.asList(1,-1,3);


        PCollection<Integer> compare =  pipeline.apply(Create.of(LINES)).
                apply(ParDo.of(new AddToInput.AddX()));

        PAssert.that(compare).containsInAnyOrder(11,9,13);


        // Run the pipeline.
        pipeline.run();
    }
}
