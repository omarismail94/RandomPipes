/*
Takes two files, emails.txt and phones.txt and flattens them into one PCollection
 */

package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Flattenizer {

    public interface MyOptions extends PipelineOptions{
        @Description("Path of the email file to read from")
        @Default.String("emails.txt")
        ValueProvider<String> getEmailFile();
        void setEmailFile(ValueProvider<String> value);

        @Description("Path of the phone file to read from")
        @Default.String("phones.txt")
        ValueProvider<String> getPhoneFile();
        void setPhoneFile(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/Flat")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);


    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runFlatten(options);
    }

    private static void runFlatten(MyOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> emails = pipeline.apply(TextIO.read().from(options.getEmailFile())).
                apply(ParDo.of(new makeKV()));


        PCollection<KV<String, String>> phones = pipeline.apply(TextIO.read().from(options.getPhoneFile())).
                apply(ParDo.of(new makeKV()));

        PCollectionList<KV<String,String>> collection = PCollectionList.of(emails).and(phones);

        collection.apply(Flatten.pCollections()).apply(ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public  void processElement(ProcessContext c){
                c.output(c.element().getKey() + " : " + c.element().getValue());
            }
        })).apply(TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }



    private static class makeKV extends DoFn<String, KV<String,String>> {
        @ProcessElement
        public void processElement(ProcessContext c){
            String[] e = c.element().split(",");
            c.output(KV.of(e[0],e[1]));
        }
    }
}
