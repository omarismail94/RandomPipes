package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BatchyBatch {
    public interface MyOptions extends PipelineOptions {
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runBatchy(options);
    }

    private static void runBatchy(MyOptions options) {
        final Logger LOG = LoggerFactory.getLogger(BatchyBatch.class);


        Pipeline p = Pipeline.create(options);

        p.apply(GenerateSequence.from(1).to(1000000000)).
                apply(WithKeys.<String,Long>of("000").withKeyType(TypeDescriptors.strings())).
                apply(GroupIntoBatches.ofSize(100)).
                apply(ParDo.of(new DoFn<KV<String, Iterable<Long>>, Long>() {
                       @ProcessElement
                        public void  processElement(ProcessContext input) {
                           int  count=0;
                            for(Long item: input.element().getValue()){
                                    count++;
                            }
                            LOG.error(Integer.toString(count));
                        }
                }));

        p.run();
    }

}
