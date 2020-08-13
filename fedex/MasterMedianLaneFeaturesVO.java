package org.omar.fedex;


import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Utf8;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//THIS IS THE ADDRESS
@DefaultCoder(MasterMedianLaneFeaturesVOCoder.class)
class MasterMedianLaneFeaturesVO   {
  transient static final TupleTag<MasterMedianLaneFeaturesVO> TUPLE_TAG = new TupleTag<MasterMedianLaneFeaturesVO>();
  private final String address;
  private final int originId;
  private final String name;

  public MasterMedianLaneFeaturesVO(String name, String s) {
    this.name = name;
    this.address = s;
    this.originId = s.length();
  }

  public String getAddress() {
    return address;
  }

  public String getName() {
    return name;
  }

  public int getOriginId() {
    return originId;
  }

  public static class KeyFn implements  SerializableFunction<MasterMedianLaneFeaturesVO,String> {
    @Override
    public String apply(MasterMedianLaneFeaturesVO input) {
      return input.getName();
    }
  }
}


class MasterMedianLaneFeaturesVOCoder extends AtomicCoder<MasterMedianLaneFeaturesVO> {
  private static final String NAMES_SEPARATOR = "_";
  private static final Logger LOG = LoggerFactory.getLogger(MasterMedianLaneFeaturesVOCoder.class);


  private static final MasterMedianLaneFeaturesVOCoder INSTANCE = new MasterMedianLaneFeaturesVOCoder();
  private static final TypeDescriptor<MasterMedianLaneFeaturesVO> TYPE_DESCRIPTOR = new TypeDescriptor<MasterMedianLaneFeaturesVO>() {
  };

  public static MasterMedianLaneFeaturesVOCoder of() {
    return INSTANCE;
  }

  private static void writeString(String value, OutputStream dos) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    VarInt.encode(bytes.length, dos);
    dos.write(bytes);
  }

  private static MasterMedianLaneFeaturesVO readString(InputStream dis) throws IOException {
    int len = VarInt.decodeInt(dis);
    if (len < 0) {
      throw new CoderException("Invalid encoded string length: " + len);
    } else {
      byte[] bytes = new byte[len];
      ByteStreams.readFully(dis, bytes);
      String serializedPerson = new String(bytes, StandardCharsets.UTF_8);
      String[] names = serializedPerson.split(NAMES_SEPARATOR,2);
      LOG.info("The decoder for MASTER sees:   "  + Arrays.toString(names) );

      return new MasterMedianLaneFeaturesVO(names[0],names[1]);
    }
  }

  private MasterMedianLaneFeaturesVOCoder() {
  }

  public void encode(MasterMedianLaneFeaturesVO value, OutputStream outStream) throws IOException {
    String serializable = value.getName()+NAMES_SEPARATOR+value.getAddress();
    this.encode(serializable, outStream, Context.NESTED);
  }

  public void encode(String value, OutputStream outStream, Context context) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    } else {
      if (context.isWholeStream) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (outStream instanceof ExposedByteArrayOutputStream) {
          ((ExposedByteArrayOutputStream)outStream).writeAndOwn(bytes);
        } else {
          outStream.write(bytes);
        }
      } else {
        writeString(value, outStream);
      }

    }
  }

  public MasterMedianLaneFeaturesVO decode(InputStream inStream) throws IOException {
    return this.decode(inStream, Context.NESTED);
  }

  public MasterMedianLaneFeaturesVO decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      byte[] bytes = StreamUtils.getBytesWithoutClosing(inStream);
      String serializedPerson = new String(bytes, StandardCharsets.UTF_8);
      String[] names = serializedPerson.split(NAMES_SEPARATOR,2);
      // LOG.info("The decoder for MASTER sees:   "  + Arrays.toString(names) );
      return new MasterMedianLaneFeaturesVO(names[0],names[1]);
    } else {
      try {
        return readString(inStream);
      } catch (UTFDataFormatException | EOFException var4) {
        throw new CoderException(var4);
      }
    }
  }

  public void verifyDeterministic() {
  }

  public boolean consistentWithEquals() {
    return true;
  }

  public TypeDescriptor<MasterMedianLaneFeaturesVO> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  public long getEncodedElementByteSize(String value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    } else {
      int size = Utf8.encodedLength(value);
      return (long)VarInt.getLength(size) + (long)size;
    }
  }
}

