/*
Reads numbers.txt file, converts to ints and only takes even numbers, then adds 10 to each of the numbers
 */

package org.omar;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class SimpleParDo {

    public interface MyOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("numbers.txt")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Validation.Required
        @Default.String("results/Newton")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        runAdd(options);

    }


    private static void runAdd(MyOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("read the file", TextIO.read().from(options.getInputFile()).withDelimiter(new byte[] {',','\n'})).
                apply("toInt", ParDo.of(new CovertToInt())).
                apply("Add", ParDo.of(new AddX())).
                apply("Convert to String", ParDo.of(new ConvertToString())).
            apply("output the file", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();

    }

    static class AddX extends DoFn<Integer, Integer> {

        @ProcessElement
        public void processElement(@Element Integer inNum, OutputReceiver<Integer> outNum) {
            outNum.output(inNum + 10);
        }
    }

    public static class CovertToInt extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(@Element String stringNum, OutputReceiver<Integer> outNum){
            int testEven = Integer.parseInt(stringNum);
            if (testEven%2 == 0) outNum.output(testEven);
        }
    }

    public static class ConvertToString extends  DoFn<Integer,String> {
        @ProcessElement
        public void processElement(@Element Integer inNum, OutputReceiver<String> outString){
            outString.output(inNum.toString());
        }

    }
}
