package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;

import java.io.Serializable;

@AutoValue
public abstract class KeyPart implements Serializable {
  public static KeyPart create(String field, boolean desc) {
    return new AutoValue_KeyPart(field, desc);
  }

  public static KeyPart asc(String field) {
    return create(field, false);
  }

  public static KeyPart desc(String field) {
    return create(field, true);
  }


  public abstract String getField();

  public abstract boolean isDesc();


}
