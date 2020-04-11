package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

/*
Multiple Outputs from DoFn, then combine 2 PCollections, outputting to a file, and the third outputted separately
 */
public class MultipleOutputs {
    public interface MyOptions extends PipelineOptions{
        @Validation.Required
        @Default.String("bible.txt")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/TheWord")
        ValueProvider<String> getGood();
        void setGood(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/TheBad")
        ValueProvider<String> getBad();
        void setBad(ValueProvider<String> value);
    }


    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runMultiple(options);
    }

    private static void runMultiple(MyOptions options) {

        final TupleTag<String> wordWithGod = new TupleTag<String>(){};
        final TupleTag<String> wordWithJesus = new TupleTag<String>(){};
        final TupleTag<String> wordWithJudas = new TupleTag<String>(){};

        Pipeline p = Pipeline.create(options);
        PCollection<String> wordLines =  p.apply("Read from file", TextIO.read().from(options.getInputFile()));

        PCollectionTuple results = wordLines.apply("Split Sentences",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String sentence, MultiOutputReceiver out){

                if (sentence.contains("God")){
                    out.get(wordWithGod).output(sentence);
                } else if (sentence.contains("Jesus")){
                    out.get(wordWithJesus).output(sentence);
                } else if (sentence.contains("Judas")){
                    out.get(wordWithJudas).output(sentence);
                }
            }
        }).withOutputTags(wordWithGod, TupleTagList.of(wordWithJesus).and(wordWithJudas)));

        PCollection<String> wordJudas = results.get(wordWithJudas);

        PCollectionList<String> collections = PCollectionList.of(results.get(wordWithGod)).and(results.get(wordWithJesus));
        PCollection<String> merged = collections.apply("Merge the Good", Flatten.<String>pCollections());

        merged.apply("Write Good", TextIO.write().to(options.getGood()));
        wordJudas.apply("Write Bad", TextIO.write().to(options.getBad()));

        p.run().waitUntilFinish();
    }

}
