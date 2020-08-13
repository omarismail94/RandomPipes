package org.omar.fedex;


import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaBeanSchema.class)
public class ModelInputVO  {

  private ThinMessageVO phone;
  private MasterMedianLaneFeaturesVO address;

  @SchemaCreate
  public ModelInputVO(ThinMessageVO msg,  MasterMedianLaneFeaturesVO laneCurrentState) {
    this.phone = msg;
    this.address = laneCurrentState;

  }

  public ThinMessageVO getPhone() {
    return phone;
  }

  public MasterMedianLaneFeaturesVO getAddress() {
    return address;
  }


}
