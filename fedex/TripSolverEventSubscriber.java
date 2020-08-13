
package org.omar.fedex;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class TripSolverEventSubscriber extends
    PTransform<PBegin, PCollection<ThinMessageVO>> {

  private final String subscriber;
  public TripSolverEventSubscriber(String tripSolverSubscriptionName) {
    this.subscriber = tripSolverSubscriptionName;
  }

  @Override
  public PCollection<ThinMessageVO> expand(PBegin input) {
    PCollection<PubsubMessage> omar = input.apply(PubsubIO.readMessagesWithAttributes().fromTopic(subscriber));
    PCollection<ThinMessageVO> bob = omar.apply(ParDo.of(new ConvertMe())).setCoder(ThinMessageVOCoder.of());
    return bob;

  }

  private static class ConvertMe extends DoFn<PubsubMessage, ThinMessageVO>
  {
    @ProcessElement
    public void processElement(ProcessContext c){
      String name = c.element().getAttribute("name");
      String phone = c.element().getAttribute("phone");
      c.output(new ThinMessageVO(name, phone));
    }

  }

}

