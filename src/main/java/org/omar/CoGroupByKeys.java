package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class CoGroupByKeys {


    public interface MyOptions extends PipelineOptions{
        @Description("Path of the email file to read from")
        @Default.String("emails.txt")
        ValueProvider<String> getEmailFile();
        void setEmailFile(ValueProvider<String> value);

        @Description("Path of the phone file to read from")
        @Default.String("phones.txt")
        ValueProvider<String> getPhoneFile();
        void setPhoneFile(ValueProvider<String> value);

        @Description("Path of the file to write to")
        @Default.String("results/CGBK/")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runCoGroup(options);
    }

    private static void runCoGroup(MyOptions options) {
        final TupleTag<String> emailsTag = new TupleTag<>();
        final TupleTag<String> phonesTag = new TupleTag<>();

        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> emails = pipeline.apply(TextIO.read().from(options.getEmailFile())).
                apply(ParDo.of(new makeKV()));


        PCollection<KV<String, String>> phones = pipeline.apply(TextIO.read().from(options.getPhoneFile())).
                apply(ParDo.of(new makeKV()));

        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple.of(emailsTag, emails)
                        .and(phonesTag, phones)
                        .apply(CoGroupByKey.create());


        PCollection<String> contactLines = results.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, CoGbkResult> e = c.element();
                String name = e.getKey();
                Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                c.output(name + " : " + emailsIter + " : " + phonesIter);
             }
        }));

        contactLines.apply(TextIO.write().to(options.getOutput()));

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

