package org.omar;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class AdvancedCombine {

    public interface MyOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("wordkeys.txt")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/AdvComb")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runAdvCombine(options);

    }


    private static void runAdvCombine(MyOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("read the file", TextIO.read().from(options.getInputFile())).
                apply(ParDo.of(new makeKV())).
                apply("Combine", Combine.<String,Integer,Double>perKey(new AverageFn())).
                apply("Convert to String", ParDo.of(new FormatAsText())).
                apply("output the file", TextIO.write().to(options.getOutput()));

        p.run();

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


    public static class FormatAsText extends DoFn<KV<String, Double>, String> {
        @ProcessElement
        public void processElement(ProcessContext input) {
            input.output(input.element().getKey() + ": " + input.element().getValue().toString());
        }
    }


}


class AverageFn extends Combine.CombineFn<Integer,AverageFn.Accum,Double> {
    public static class Accum implements Serializable {
        int sum = 0;
        int count = 0;
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input) {
        accum.sum+= input;
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {

        Accum everything = new Accum();
        for (Accum i : accumulators){
            everything.sum += i.sum;
            everything.count += i.count;
        }
        return everything;
    }

    @Override
    public Double extractOutput(Accum accumulator) {
        return ((double) accumulator.sum)/accumulator.count;
    }
}
