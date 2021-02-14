package org.omar.PubSub;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.json.JSONObject;

public class PubSub_CoGBK_GCS {

  static final TupleTag<KV<String, String>> nameTag = new TupleTag<KV<String, String>>() {
  };
  static final TupleTag<KV<String, String>> productTag = new TupleTag<KV<String, String>>() {
  };
  static final TupleTag<KV<String, String>> cityTag = new TupleTag<KV<String, String>>() {
  };

  public interface MyOptions extends PipelineOptions {

    @Description("Subscription URI")
    @Required
    @Default.String("projects/nerdynerd/subscriptions/data")
    String getSubscription();

    void setSubscription(String value);


    @Description("Bucket Location")
    @Required
    @Default.String("gs://mcbuckety")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    runPipy(options);
  }

  private static void runPipy(MyOptions options) {

    Pipeline p = Pipeline.create(options);

    PCollection<PubsubMessage> messages = null;

    messages = p.apply("Read PubSub Subscription",
        PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubscription())).apply(
        Window.<PubsubMessage>into(new GlobalWindows()).triggering(Repeatedly.forever(
            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(10))))
            .discardingFiredPanes());


    PCollectionTuple convertedToTuple = messages
        .apply("Split to Tuples",
            ParDo.of(new PubsubMessageToFailsafeElementFn())
                .withOutputTags(nameTag, TupleTagList.of(cityTag).and(productTag)));

    PCollection<KV<String, String>> hi = convertedToTuple.get(nameTag);
    PCollection<KV<String, String>> si = convertedToTuple.get(productTag);
    PCollection<KV<String, String>> di = convertedToTuple.get(cityTag);

    final TupleTag<String> t1 = new TupleTag<>();
    final TupleTag<String> t2 = new TupleTag<>();
    final TupleTag<String> t3 = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(t1, hi).and(t2, si).and(t3, di)
            .apply(CoGroupByKey.create());

    PCollection<String> contactLines = results
        .apply("Create array", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            KV<String, CoGbkResult> e = c.element();
            String name = e.getKey();
            Iterable<String> emailsIter = e.getValue().getAll(t1);
            Iterable<String> phonesIter = e.getValue().getAll(t2);
            Iterable<String> cityIter = e.getValue().getAll(t3);

            c.output(name + " : " + emailsIter + " : " + phonesIter + " : " + cityIter);
          }
        }));

    contactLines
        .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(options.getOutput()));

    p.run();


  }


  private static class PubsubMessageToFailsafeElementFn extends
      DoFn<PubsubMessage, KV<String, String>> {


    @ProcessElement
    public void processElement(@Element PubsubMessage message, MultiOutputReceiver out) {

      byte[] text = message.getPayload();
      JSONObject testV = new JSONObject(new String(text));
      String name = testV.get("first_name").toString();
      String state = testV.get("state").toString();
      String product = testV.get("product").toString();
      String city = testV.get("city").toString();

      out.get(nameTag).output(KV.of(state, name));
      out.get(productTag).output(KV.of(state, product));
      out.get(cityTag).output(KV.of(state, city));


    }
  }
}
