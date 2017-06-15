package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.DatastoreToGcs.EntityToJson;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 6/15/17.
 */
public class DatastoreToGcsTest {
  public static final String jsTransformPath = "gs://teleport-test/test/transforms/good";
  public static final String mEntityJson = "{\"key\":{\"partitionId\":{\"projectId\":\"strong-moose\"},\"path\":[{\"kind\":\"Drawing\",\"name\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"}]},\"properties\":{\"points\":{\"arrayValue\":{\"values\":[{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"219\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"242\"},\"x\":{\"integerValue\":\"351\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"255\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"267\"},\"x\":{\"integerValue\":\"347\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"281\"},\"x\":{\"integerValue\":\"345\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"289\"},\"x\":{\"integerValue\":\"344\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"293\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"296\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"298\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"345\"}}}}]}},\"drawingId\":{\"stringValue\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"},\"canvasId\":{\"stringValue\":\"79a1d9d9-e255-427a-9b09-f45157e97790\"}}}";
  public static final String mTransformedEntityJson = "{\"Some Property\":\"Some Key\",\"entity jsonified\":\"{\\\"key\\\":{\\\"partitionId\\\":{\\\"projectId\\\":\\\"strong-moose\\\"},\\\"path\\\":[{\\\"kind\\\":\\\"Drawing\\\",\\\"name\\\":\\\"31ce830e-91d0-405e-855a-abe416cadc1f\\\"}]},\\\"properties\\\":{\\\"points\\\":{\\\"arrayValue\\\":{\\\"values\\\":[{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"219\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"349\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"242\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"351\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"255\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"349\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"267\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"347\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"281\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"345\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"289\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"344\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"293\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"342\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"296\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"341\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"298\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"341\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"299\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"341\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"299\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"342\\\"}}}},{\\\"entityValue\\\":{\\\"properties\\\":{\\\"y\\\":{\\\"integerValue\\\":\\\"299\\\"},\\\"x\\\":{\\\"integerValue\\\":\\\"345\\\"}}}}]}},\\\"drawingId\\\":{\\\"stringValue\\\":\\\"31ce830e-91d0-405e-855a-abe416cadc1f\\\"},\\\"canvasId\\\":{\\\"stringValue\\\":\\\"79a1d9d9-e255-427a-9b09-f45157e97790\\\"}}}\"}";

  @Test
  public void testDatastoreToGcs_EntityToJson_noTransform() throws Exception {
    // No Transforms
    StaticValueProvider<String> valueProvider = StaticValueProvider.of(null);
    DoFnTester<Entity, String> fnTester = DoFnTester.of(new EntityToJson(valueProvider));

    Builder entityBuilder = Entity.newBuilder();
    JsonFormat.parser().usingTypeRegistry(
        TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build())
        .merge(mEntityJson, entityBuilder);

    Entity entity = entityBuilder.build();
    List<String> entityJsonOutputs = fnTester.processBundle(entity);
    Assert.assertEquals(mEntityJson, entityJsonOutputs.get(0));
  }

  @Test
  public void testDatastoreToGcs_EntityToJson_withTransform() throws Exception {
    // No Transforms
    StaticValueProvider<String> valueProvider = StaticValueProvider.of(jsTransformPath);
    DoFnTester<Entity, String> fnTester = DoFnTester.of(new EntityToJson(valueProvider));

    Builder entityBuilder = Entity.newBuilder();
    JsonFormat.parser().usingTypeRegistry(
        TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build())
        .merge(mEntityJson, entityBuilder);

    Entity entity = entityBuilder.build();
    List<String> entityJsonOutputs = fnTester.processBundle(entity);
    Assert.assertEquals(mTransformedEntityJson, entityJsonOutputs.get(0));
  }
}
