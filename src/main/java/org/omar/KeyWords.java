package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;


public class KeyWords {

    public interface MyOptions extends PipelineOptions{

        @Validation.Required
        @Default.String("wordkeys.txt")
        String getInputFile();
        void setInputFile(String value);

        @Validation.Required
        @Default.String("results/Euclid")
        String getOutputDirectory();
        void setOutputDirectory(String value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runKeyWords(options);
    }

    private static void runKeyWords(MyOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from(options.getInputFile())).
               apply(ParDo.of(new makeKV())).
               apply(GroupByKey.create()).
               apply(ParDo.of(new FormatAsText())).
                apply(TextIO.write().to(options.getOutputDirectory()));

                p.run().waitUntilFinish();
    }

    private static class makeKV extends DoFn<String, KV<String,Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c){
            String[] e = c.element().split(",");
            String name = e[0];
            Integer number = Integer.parseInt(e[1]);
            c.output(KV.of(name,number));
        }
    }

    public static class FormatAsText extends DoFn<KV<String, Iterable<Integer>>, String> {
        @ProcessElement
          public void processElement(ProcessContext input) {
            input.output(input.element().getKey() + ": " + input.element().getValue());
        }
    }

}
