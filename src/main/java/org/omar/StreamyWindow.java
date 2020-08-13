package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamyWindow {
    public interface MyOptions extends PipelineOptions {
        @Description("Path of the output file including its filename prefix.")
        @Required
        @Default.String("gs://mcbuckety/results/James")
        String getOutput();
        void setOutput(String value);

    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runStreamy(options);
    }

    private static void runStreamy(MyOptions options) {
        final Logger LOG = LoggerFactory.getLogger(StreamyWindow.class);


        Pipeline p = Pipeline.create(options);

        p.apply(GenerateSequence.from(1).to(10000)).
                apply(WithTimestamps.of( (Long record) -> new Instant(System.currentTimeMillis()))).
                apply(WithKeys.<String,Long>of("000").withKeyType(TypeDescriptors.strings())).
                apply(Window.into(FixedWindows.of(Duration.standardSeconds(1)))).
                apply(GroupIntoBatches.ofSize(100)).
                apply(ParDo.of(new DoFn<KV<String, Iterable<Long>>, String>() {
                    @ProcessElement
                    public void  processElement(ProcessContext input) {
                        int  count=0;
                        for(Long item: input.element().getValue()){
                            count++;
                        }
                        input.output(Integer.toString(count));
                    }
                })).apply(TextIO.write().to(options.getOutput()));

        p.run();
    }

}

