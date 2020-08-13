package org.omar.fedex;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatefulProbEddPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulProbEddPipeline.class);
    private MLModelPipelineOptions options;
    private Pipeline pipeline;

    public static void main(String[] args) {
        StatefulProbEddPipeline pipe = new StatefulProbEddPipeline();
        pipe.init(args);
        pipe.build();
        pipe.run();
    }

    public void init(String[] args) {
        this.options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MLModelPipelineOptions.class);
        this.pipeline = Pipeline.create(options);
    }

    public void build() {


        PCollection<KV<String,MasterMedianLaneFeaturesVO>> lanes = pipeline.apply("Get Master Lane Events", new MasterLaneEventSubscriber(options.getMasterLaneSubscriptionName()))
            .apply("Master Lane  KV object", WithKeys.of(new MasterMedianLaneFeaturesVO.KeyFn()))
            .apply("Window Master Lane data", Window.<KV<String,MasterMedianLaneFeaturesVO>>into(new GlobalWindows())
                .triggering(Repeatedly
                    .forever(AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(60))
                    )
                )
                .withAllowedLateness(Duration.ZERO).discardingFiredPanes());

        PCollection<KV<String, ThinMessageVO>> messages = pipeline.apply("Get Trip Solver Events", new TripSolverEventSubscriber(options.getTripSolverSubscriptionName()))
            .apply("Trip Solver KV object", WithKeys.of(new ThinMessageVO.KeyFn()))
            .apply("Window trip solver event/ afterprocessingtime, allow lateness 0,delay 5 seconds, discarding", Window.<KV<String, ThinMessageVO>>into(new GlobalWindows())
                .triggering(Repeatedly
                    .forever(AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(60))
                    )
                )
                .withAllowedLateness(Duration.ZERO).discardingFiredPanes());


         KeyedPCollectionTuple.of(ThinMessageVO.TUPLE_TAG, messages)
            .and(MasterMedianLaneFeaturesVO.TUPLE_TAG, lanes)
            .apply("Co group the msg and lane data", CoGroupByKey.create())
            .apply("Filter out empty keys", ParDo.of(new FilterEmptyData()))
            .apply("Join msg and lane data", ParDo.of(new ModelInputJoinProcessor()))
            .apply("Print the Model" , ParDo.of(new PrintData()))
            .apply(
                FileIO
                    .<String,String>writeDynamic()
                    .by(String::toString)
                    .withDestinationCoder(StringUtf8Coder.of())
                    .withNaming(
                        Contextful.fn(
                            (entityName) -> FileIO.Write.defaultNaming("folderName" + "/" + entityName, ".txt")))
                    .via(TextIO.sink())
                    .to(options.getOutput()).withNumShards(1));
    }

    private static class PrintData extends DoFn<ModelInputVO,String> {
        @ProcessElement
        public void processElement(@Element ModelInputVO outputV0, OutputReceiver<String> out){
            LOG.info("I am emitting: " + outputV0.getAddress().getAddress() + " " + outputV0.getPhone().getPhoneNumber());
            out.output(outputV0.getAddress().getAddress() + " " + outputV0.getPhone().getPhoneNumber());
        }

    }

    private static class FilterEmptyData extends DoFn<KV<String, CoGbkResult>, KV<String, CoGbkResult>> {
        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element KV<String, CoGbkResult> inputVO, OutputReceiver<KV<String, CoGbkResult>> out) {

            LOG.info("filter empty data: " + inputVO.getKey() + " " + inputVO.getValue().getAll(MasterMedianLaneFeaturesVO.TUPLE_TAG) + " " +
                inputVO.getValue().getAll(ThinMessageVO.TUPLE_TAG));
            if (inputVO.getKey().length() != 0) {
                out.output(inputVO);
            }
            else
            {
                //        System.out.println("JRV - filter out msg: " + inputVO.getValue().getAll(MessageVO.TUPLE_TAG));
                //        System.out.println("JRV - filter out lane: " + inputVO.getValue().getAll(MasterMedianLaneFeaturesVO.TUPLE_TAG));
            }

        }
    }

    public PipelineResult run() {
        //System.out.println("JRV - should be running");
        return pipeline.run();
    }


}