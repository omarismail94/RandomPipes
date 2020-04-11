package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SideInput {
    public interface MyOptions extends PipelineOptions{
        @Validation.Required
        @Default.String("bible.txt")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/James")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runBooky(options);
    }

    private static void runBooky(MyOptions options) {
        Pipeline p = Pipeline.create(options);

        PCollection<String> wordLines =  p.apply("Read from file", TextIO.read().from(options.getInputFile()));

        PCollection<Integer> wordLengths = wordLines.apply("Calculate Word Length", ParDo.of(new DoFn<String, Integer>() {
            @ProcessElement
            public void processElement(@Element String word, OutputReceiver<Integer> lengthy){
                lengthy.output(word.split("\\s+").length);
            }

        }));

        final PCollectionView<Integer> maxWordLength = wordLengths.apply("Find Max", Combine.globally(Max.ofIntegers()).asSingletonView());

        PCollection<String> wordsBelowCutOff =
                wordLines.apply("Cutoff", ParDo
                        .of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
                                int lengthCutOff = c.sideInput(maxWordLength);
                                if (word.split("\\s+").length == lengthCutOff) {
                                    out.output(word);
                                }
                            }
                        }).withSideInputs(maxWordLength)
                );

        wordsBelowCutOff.apply("Write to disk", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }

    public static class ConvertToString extends  DoFn<Integer,String> {
        @ProcessElement
        public void processElement(@Element Integer inNum, OutputReceiver<String> outString){
            outString.output(inNum.toString());
        }

    }
}
