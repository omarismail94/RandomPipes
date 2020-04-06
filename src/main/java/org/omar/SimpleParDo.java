package org.omar;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class SimpleParDo {

    public interface MyOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("numbers.txt")
        String getInputFile();
        void setInputFile(String value);

        @Validation.Required
        @Default.String("results/Newton")
        String getOutputDirectory();
        void setOutputDirectory(String value);
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
            apply("output the file", TextIO.write().to(options.getOutputDirectory()));

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
