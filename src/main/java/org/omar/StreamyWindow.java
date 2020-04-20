package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamyWindow {
    public interface MyOptions extends PipelineOptions {
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runStreamy(options);
    }

    private static void runStreamy(MyOptions options) {
        final Logger LOG = LoggerFactory.getLogger(StreamyWindow.class);


        Pipeline p = Pipeline.create(options);

        p.apply(GenerateSequence.from(1).to(1000000000)).
                apply(WithTimestamps.of( (Long record) -> new Instant(System.currentTimeMillis()))).
                apply(WithKeys.<String,Long>of("000").withKeyType(TypeDescriptors.strings())).
                apply(Window.into(FixedWindows.of(Duration.standardSeconds(1)))).
                apply(GroupIntoBatches.ofSize(100)).
                apply(ParDo.of(new DoFn<KV<String, Iterable<Long>>, Long>() {
                    @ProcessElement
                    public void  processElement(ProcessContext input) {
                        int  count=0;
                        for(Long item: input.element().getValue()){
                            count++;
                        }
                        LOG.info(Integer.toString(count));
                    }
                }));

        p.run();
    }

}
