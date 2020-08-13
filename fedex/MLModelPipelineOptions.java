package org.omar.fedex;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface MLModelPipelineOptions extends PipelineOptions, StreamingOptions {
  @Description("The Cloud Pub/Sub topic to read from.")
  @Required
  @Default.String("projects/nerdynerd/topics/namePhone")
  String getTripSolverSubscriptionName();
  void setTripSolverSubscriptionName(String value);

  @Description("The Cloud Pub/Sub topic to read from.")
  @Required
  @Default.String("projects/nerdynerd/topics/nameAddress")
  String getMasterLaneSubscriptionName();
  void setMasterLaneSubscriptionName(String value);


  @Description("Path of the output file including its filename prefix.")
  @Required
  @Default.String("gs://ismailoplay/results/James")
  String getOutput();
  void setOutput(String value);
}
