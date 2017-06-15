package com.google.cloud.dataflow.teleport.Helpers;

import java.util.List;
import javax.script.Invocable;
import javax.script.ScriptException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the JSTransform Class
 */
public class JSTransformTest {
  public static final String gcsTransformFns = "gs://strong-moose.appspot.com/transforms";

  @Test
  public void testJSTransform_getScripts() {
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(gcsTransformFns)
        .build();

    List<String> scripts = jsTransform.getScripts();
    Assert.assertEquals(4, scripts.size());

    JSTransform jsTransformGood = JSTransform.newBuilder()
        .setGcsJSPath(gcsTransformFns + "/good_script/")
        .build();

    List<String> goodScripts = jsTransformGood.getScripts();
    Assert.assertEquals(1, goodScripts.size());
  }


  @Test
  public void testJSTransform_getInvocable() throws ScriptException, NoSuchMethodException {
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(gcsTransformFns + "/good_script/")
        .build();

    String output = (String) jsTransform.invoke( "{\"key\": \"value\"}");
    String expected = "{\"Some Property\":\"Some Key\",\"entity jsonified\":\"{\\\"key\\\": \\\"value\\\"}\"}";
    Assert.assertEquals(expected, output);
  }
}
