package com.google.cloud.dataflow.teleport;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Common Dataflow Options across all pipelines
 */
public interface CommonOptions extends PipelineOptions {

  @Validation.Required
  @Description("ProjectId")
  ValueProvider<String> getProjectId();
  void setProjectId(ValueProvider<String> projectId);

  @Description("Block until job finishes")
  @Default.Boolean(true)
  Boolean getKeepJobsRunning();
  void setKeepJobsRunning(Boolean keepJobsRunning);
}
