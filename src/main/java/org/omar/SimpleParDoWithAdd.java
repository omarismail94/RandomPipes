package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/*
Unlike SimpeParDo, you can add/subtract any number you would like in this class
 */

public class SimpleParDoWithAdd {
    public interface MyOptions extends PipelineOptions{
        @Validation.Required
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        @Default.Integer(10)
        ValueProvider<Integer> getAdder();
        void setAdder(ValueProvider<Integer> value);

        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runSidy(options);
    }

    private static void runSidy(MyOptions options) {
        Pipeline p = Pipeline.create(options);



        p.apply("read the file", TextIO.read().from(options.getInputFile()).withDelimiter(new byte[] {',','\n'})).
                apply("toInt", ParDo.of(new CovertToInt())).
                apply("Add", ParDo.of(new DoFn<Integer, Integer>() {
                    @ProcessElement
                    public void processElement(@Element Integer inNum, OutputReceiver<Integer> outNum, PipelineOptions allTheOptions) {
                        MyOptions opts = allTheOptions.as(MyOptions.class);
                        outNum.output(inNum + opts.getAdder().get());
                    }
                })).
                apply("Convert to String", ParDo.of(new ConvertToString())).
                apply("output the file", TextIO.write().to(options.getOutput()).withSuffix(".txt"));

        p.run();

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
