/*
  Copyright 2017 Google Inc.
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.Helpers.JSTransform;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS to Datastore Records
 */
public class GcsToDatastore {
  private static final Logger mLogger = LoggerFactory.getLogger(DatastoreToGcs.class);

  /**
   * Runs the DatastoreToGcs dataflow pipeline
   */
  public static void main(String[] args) throws IOException, ScriptException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("IngestJson", TextIO.read()
            .from(options.getJsonPathPrefix()))
        .apply("GcsToEntity", ParDo.of(new JsonToEntity(options.getJsTransformPath())))
        .apply(DatastoreIO.v1().write()
            .withProjectId(options.getDatastoreProject()));

    pipeline.run();
  }

  interface Options extends GcpOptions {
    @Validation.Required
    @Description("GCS Data Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getJsonPathPrefix();
    void setJsonPathPrefix(ValueProvider<String> jsonPathPrefix);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);

    @Description("Project to save Datastore Entities in")
    ValueProvider<String> getDatastoreProject();
    void setDatastoreProject(ValueProvider<String> datastoreProject);
  }

  /**
   * Converts a Protobuf Encoded Json String to a Datastore Entity
   */
  static class JsonToEntity extends DoFn<String, Entity> {
    protected JsonFormat.Parser mJsonParser;
    protected JSTransform mJSTransform;
    protected ValueProvider<String> mJsTransformPath;

    public JsonToEntity(ValueProvider<String> jsTransformPath) {
      mJsTransformPath = jsTransformPath;
    }

    private JsonFormat.Parser getJsonParser() {
      if (mJsonParser == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonParser = JsonFormat.parser()
            .usingTypeRegistry(typeRegistry);
      }
      return mJsonParser;
    }

    private JSTransform getJSTransform() throws ScriptException {
      if (mJSTransform == null) {
        String jsTransformPath = "";
        if (mJsTransformPath.isAccessible()) {
          jsTransformPath = mJsTransformPath.get();
        }

        mJSTransform = JSTransform.newBuilder()
            .setGcsJSPath(jsTransformPath)
            .build();
      }
      return mJSTransform;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String entityJson = c.element();

      if (getJSTransform().hasTransform()) {
        entityJson = getJSTransform().invoke(entityJson);
      }

      Builder builder = Entity.newBuilder();
      getJsonParser().merge(entityJson, builder);
      Entity entity = builder.build();
      c.output(entity);
    }
  }
}
