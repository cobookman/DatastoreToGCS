package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.GcsToDatastore.JsonToEntity;
import com.google.datastore.v1.Entity;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bookman on 6/15/17.
 */
public class GcsToDatastoreTest {
  public static final String mEntityJson = "{\"key\":{\"partitionId\":{\"projectId\":\"strong-moose\"},\"path\":[{\"kind\":\"Drawing\",\"name\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"}]},\"properties\":{\"points\":{\"arrayValue\":{\"values\":[{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"219\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"242\"},\"x\":{\"integerValue\":\"351\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"255\"},\"x\":{\"integerValue\":\"349\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"267\"},\"x\":{\"integerValue\":\"347\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"281\"},\"x\":{\"integerValue\":\"345\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"289\"},\"x\":{\"integerValue\":\"344\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"293\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"296\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"298\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"341\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"342\"}}}},{\"entityValue\":{\"properties\":{\"y\":{\"integerValue\":\"299\"},\"x\":{\"integerValue\":\"345\"}}}}]}},\"drawingId\":{\"stringValue\":\"31ce830e-91d0-405e-855a-abe416cadc1f\"},\"canvasId\":{\"stringValue\":\"79a1d9d9-e255-427a-9b09-f45157e97790\"}}}";

  @Test
  public void testGcsToDatastore_EntityToJson_noTransform() throws Exception {
    StaticValueProvider<String> valueProvider = StaticValueProvider.of(null);
    DoFnTester<String, Entity> fnTester = DoFnTester.of(new JsonToEntity(valueProvider));
    List<Entity> output = fnTester.processBundle(mEntityJson);
    Entity outputEntity = output.get(0);

    Printer printer = JsonFormat.printer()
        .omittingInsignificantWhitespace()
        .usingTypeRegistry(
            TypeRegistry.newBuilder()
                .add(Entity.getDescriptor())
                .build());
    Assert.assertEquals(mEntityJson, printer.print(outputEntity));
  }
}
