package org.omar;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;


class ReadDoNothingWrite {


    public interface MyOptions extends PipelineOptions{
        @Required
        String getInputFile();
        void setInputFile(String value);

        @Required
        String getOutputDirectory();
        void setOutputDirectory(String value);
    }


    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        runOutput(options);
    }


    static void runOutput(MyOptions options){
            Pipeline p = Pipeline.create(options);

            p.apply("read the file", TextIO.read().from(options.getInputFile())).
            apply("output the file", TextIO.write().to(options.getOutputDirectory()));

            p.run().waitUntilFinish();

    }







}
