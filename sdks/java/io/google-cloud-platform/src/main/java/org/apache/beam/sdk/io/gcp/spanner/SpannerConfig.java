package org.apache.beam.sdk.io.gcp.spanner;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;

/** Configuration for a Cloud Spanner client. */
@AutoValue
public abstract class SpannerConfig implements Serializable {

  private static final long serialVersionUID = -5680874609304170301L;

  @Nullable
  public abstract String getProjectId();

  @Nullable
  public abstract String getInstanceId();

  @Nullable
  public abstract String getDatabaseId();

  @Nullable
  @VisibleForTesting
  abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

  abstract Builder toBuilder();

  SpannerOptions getSpannerOptions() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    if (getServiceFactory() != null) {
      builder.setServiceFactory(getServiceFactory());
    }
    return builder.build();
  }

  public static SpannerConfig create() {
    return builder().build();
  }

  public static Builder builder() {
    return new AutoValue_SpannerConfig.Builder();
  }

  public void validate(PipelineOptions options) {
    checkNotNull(
        getInstanceId(),
        "SpannerIO.read() requires instance id to be set with withInstanceId method");
    checkNotNull(
        getDatabaseId(),
        "SpannerIO.read() requires database id to be set with withDatabaseId method");
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Output Project"))
        .addIfNotNull(DisplayData.item("instanceId", getInstanceId()).withLabel("Output Instance"))
        .addIfNotNull(DisplayData.item("databaseId", getDatabaseId()).withLabel("Output Database"));

    if (getServiceFactory() != null) {
      builder.addIfNotNull(
          DisplayData.item("serviceFactory", getServiceFactory().getClass().getName())
              .withLabel("Service Factory"));
    }
  }

  /** Builder for {@link SpannerConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setInstanceId(String instanceId);

    public abstract Builder setDatabaseId(String databaseId);

    abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

    public abstract SpannerConfig build();
  }

  public SpannerConfig withProjectId(String projectId) {
    return toBuilder().setProjectId(projectId).build();
  }

  public SpannerConfig withInstanceId(String instanceId) {
    return toBuilder().setInstanceId(instanceId).build();
  }

  public SpannerConfig withDatabaseId(String databaseId) {
    return toBuilder().setDatabaseId(databaseId).build();
  }
}
