package org.omar;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class SimpleCombine {

    public interface MyOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runCombine(options);

    }


    private static void runCombine(MyOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("read the file", TextIO.read().from(options.getInputFile()).withDelimiter(new byte[] {',','\n'})).
                apply("toInt", ParDo.of(new CovertToInt())).
                apply("addPCollection", Combine.globally(new SumInts())).
                apply("Convert to String", ParDo.of(new ConvertToString())).
                apply("output the file", TextIO.write().to(options.getOutput()));

        p.run();

    }

    public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int item : input) {
                sum += item;
            }
            return sum;
        }
    }

    public static class CovertToInt extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(@Element String stringNum, OutputReceiver<Integer> outNum){
            outNum.output(Integer.parseInt(stringNum));
        }
    }

    public static class ConvertToString extends  DoFn<Integer,String> {
        @ProcessElement
        public void processElement(@Element Integer inNum, OutputReceiver<String> outString){
            outString.output(inNum.toString());
        }

    }
}
