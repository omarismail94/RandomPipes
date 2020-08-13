package org.omar.fedex;


import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelInputJoinProcessor extends DoFn<KV<String, CoGbkResult>, ModelInputVO> {
  private static final Logger LOG = LoggerFactory.getLogger(ModelInputJoinProcessor.class);
  private static final String TIMER_ID = "timerId";
  private static final String MESSAGE_ID = "messageId";
  private static final String LANE_ID = "laneId";

  @StateId(MESSAGE_ID)
  private final StateSpec<BagState<ThinMessageVO>> messageSpec = StateSpecs.bag(ThinMessageVOCoder.of());

  @StateId(LANE_ID)
  private final StateSpec<ValueState<MasterMedianLaneFeaturesVO>> laneSpec = StateSpecs.value(MasterMedianLaneFeaturesVOCoder.of());

  @TimerId(TIMER_ID)
  private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement
  public void process(
      ProcessContext context,
      @TimerId(TIMER_ID) Timer timerId, @AlwaysFetched @StateId(MESSAGE_ID) BagState<ThinMessageVO> messageState, @AlwaysFetched @StateId(LANE_ID) ValueState<MasterMedianLaneFeaturesVO> laneState) {

    if (context.element().getKey().length() == 0) {
      return;
    }
    MasterMedianLaneFeaturesVO laneCurrentState = laneState.read();
    for (MasterMedianLaneFeaturesVO lane : context.element().getValue().getAll(MasterMedianLaneFeaturesVO.TUPLE_TAG)) {
      if (lane.getOriginId() != 0) {
        laneState.write(lane);  // We never clear lane state
        laneCurrentState = lane;
      }
    }

    LOG.info("lane current state: " + laneCurrentState);
    if (laneCurrentState == null) {

      // We do not have lane information, so just keep storing the messages
      for (ThinMessageVO vo : context.element().getValue().getAll(ThinMessageVO.TUPLE_TAG)) {
        LOG.info("JRV - should be storing messages in state: " + vo.getName() + " " + vo.getPhoneNumber());
        //System.out.println("JRV - should be storing messages in state: " + vo);
        messageState.add(vo);
      }

      LOG.info("JRV - will delay 20 minutes for the key: " + context.element().getKey());
      timerId.offset(Duration.standardMinutes(20)).setRelative();


    } else {
      LOG.info("JRV - found a lane: " + laneCurrentState.getName() + " " + laneCurrentState.getAddress());
      for (ThinMessageVO msg : messageState.read()) {
        // If we have a message in the current state, then we were missing the lane when it got stored
        LOG.info("Should be outputting stored messages for lane: " + msg.getName() + " " + msg.getPhoneNumber());
        ModelInputVO vo = new ModelInputVO(msg, laneCurrentState);
        context.output(vo);
      }

      messageState.clear();
      for (ThinMessageVO msg : context.element().getValue().getAll(ThinMessageVO.TUPLE_TAG)) {
        LOG.info("Should be outputting new messages for lane: " + msg.getName() + " " + msg.getPhoneNumber());
        ModelInputVO vo = new ModelInputVO(msg, laneCurrentState);
        context.output(vo);
      }
    }
  }


  @OnTimer(TIMER_ID)
  public void onTimer(
      OnTimerContext c,
      @TimerId(TIMER_ID) Timer timerId, @StateId(MESSAGE_ID) BagState<ThinMessageVO> messageState, @StateId(LANE_ID) ValueState<MasterMedianLaneFeaturesVO> laneState) {

    MasterMedianLaneFeaturesVO lane = laneState.read();

    if (lane == null) {

      for (ThinMessageVO msg : messageState.read()) {
        // If we have a message in the current state, then we were missing the lane when it got stored
        LOG.info("Should be expiring for msg: " + msg.getName() + " " + msg.getPhoneNumber());
        c.output(new ModelInputVO(msg, new MasterMedianLaneFeaturesVO("NULL","NULL")));
      }
    }
    else
    {
      LOG.info("JRV - Did not expire for lane: " + lane);
    }
  }

}