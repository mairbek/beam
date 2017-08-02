package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;

import java.util.Iterator;
import java.util.List;

public class KeyCoder {

  static byte[] encode(String table, Key key, List<KeyPart> keyParts) {
    OrderedCode orderedCode = new OrderedCode();
    Iterator<Object> ai = key.getParts().iterator();
    orderedCode.writeBytes(table.getBytes());
    for (KeyPart part : keyParts) {
      Object ao = ai.next();
      if (ao == null) {
        orderedCode.writeInfinity();
      } else if (ao instanceof Boolean) {
        // TODO(mairbek): ?
        long value = ((Boolean) ao) ? 0 : 1;
        if (part.isDesc()) {
          orderedCode.writeSignedNumDecreasing(value);
        } else {
          orderedCode.writeSignedNumIncreasing(value);
        }
      }else if (ao instanceof Long) {
        Long value = (Long) ao;
        if (part.isDesc()) {
          orderedCode.writeSignedNumDecreasing(value);
        } else {
          orderedCode.writeSignedNumIncreasing(value);
        }
      } else if (ao instanceof Double) {
        Double value = (Double) ao;
        if (part.isDesc()) {
          // TODO(mairbek): wtf?
          orderedCode.writeSignedNumDecreasing(Double.doubleToLongBits(value));
        } else {
          orderedCode.writeSignedNumIncreasing(Double.doubleToLongBits(value));
        }
      } else if (ao instanceof String) {
        String value = (String) ao;
        if (part.isDesc()) {
          orderedCode.writeBytesDecreasing(value.getBytes());
        } else {
          orderedCode.writeBytes(value.getBytes());
        }
      } else if (ao instanceof ByteArray) {
        ByteArray value = (ByteArray) ao;
        if (part.isDesc()) {
          orderedCode.writeBytesDecreasing(value.toByteArray());
        } else {
          orderedCode.writeBytes(value.toByteArray());
        }
      } else if (ao instanceof Timestamp) {
        Timestamp value = (Timestamp) ao;
        if (part.isDesc()) {
          orderedCode.writeNumDecreasing(value.getSeconds());
          orderedCode.writeNumDecreasing(value.getNanos());
        } else {
          orderedCode.writeNumIncreasing(value.getSeconds());
          orderedCode.writeNumIncreasing(value.getNanos());
        }
      } else if (ao instanceof Date) {
        Date value = (Date) ao;
        if (part.isDesc()) {
          orderedCode.writeSignedNumDecreasing(value.getYear());
          orderedCode.writeSignedNumDecreasing(value.getMonth());
          orderedCode.writeSignedNumDecreasing(value.getDayOfMonth());
        } else {
          orderedCode.writeSignedNumIncreasing(value.getYear());
          orderedCode.writeSignedNumIncreasing(value.getMonth());
          orderedCode.writeSignedNumIncreasing(value.getDayOfMonth());
        }
      }
      else {
        throw new IllegalStateException("!!!!");
      }
    }
    return orderedCode.getEncodedBytes();
  }
}
