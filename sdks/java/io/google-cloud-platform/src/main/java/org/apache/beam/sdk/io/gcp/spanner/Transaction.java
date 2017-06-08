package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import java.io.Serializable;

/** A transaction object. */
@AutoValue
public abstract class Transaction implements Serializable {
  abstract Timestamp timestamp();

  public static Transaction create(Timestamp timestamp) {
    return new AutoValue_Transaction(timestamp);
  }
}
