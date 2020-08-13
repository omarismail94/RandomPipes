
package org.omar.fedex;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class MasterLaneEventSubscriber extends
    PTransform<PBegin, PCollection<MasterMedianLaneFeaturesVO>> {

  private final String subscriber;
  public MasterLaneEventSubscriber(String masterLaneSubscriptionName) {
    this.subscriber = masterLaneSubscriptionName;
  }

  @Override
  public PCollection<MasterMedianLaneFeaturesVO> expand(PBegin input) {
    PCollection<PubsubMessage> omar = input.apply(PubsubIO.readMessagesWithAttributes().fromTopic(subscriber));

    PCollection<MasterMedianLaneFeaturesVO> bob = omar.apply(
        ParDo.of(new ConvertMe())).setCoder(MasterMedianLaneFeaturesVOCoder.of());

    return bob;

  }

  private static class ConvertMe extends DoFn<PubsubMessage, MasterMedianLaneFeaturesVO>
  {
    @ProcessElement
    public void processElement(ProcessContext c){
    String name = c.element().getAttribute("name");
    String address = c.element().getAttribute("address");
    c.output(new MasterMedianLaneFeaturesVO(name, address));
  }

  }

}

