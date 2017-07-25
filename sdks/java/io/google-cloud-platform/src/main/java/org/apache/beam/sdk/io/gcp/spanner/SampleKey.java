package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Key;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

@AutoValue
public abstract class SampleKey implements Serializable, Comparable<SampleKey> {
  @Nullable
  public abstract String getTable();

  @Nullable
  public abstract Key getKey();

  @Nullable
  public abstract List<KeyPart> getKeySpec();

  public static SampleKey create(String table, Key key, List<KeyPart> keyParts) {
    return new AutoValue_SampleKey(table, key, keyParts);
  }

  @Override public int compareTo(SampleKey o) {
    int result = getTable().compareTo(o.getTable());
    if (result != 0) {
      return result;
    }

    Iterator<Object> ai = getKey().getParts().iterator();
    Iterator<Object> bi = o.getKey().getParts().iterator();
    for (KeyPart part : getKeySpec()) {
      Object ao = ai.next();
      Object bo = bi.next();
      if (ao == null) {
        // Verify
        result = bo == null ? 0 : -1;
      } else if (bo == null) {
        result = 1;
      } else {
        result = ((Comparable) ao).compareTo(bo);
      }
      if (result != 0) {
        return result * (part.isDesc() ? - 1 : 1);
      }
    }
    return 0;
  }
}
