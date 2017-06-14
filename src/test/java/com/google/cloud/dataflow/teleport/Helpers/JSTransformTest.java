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
    List<String> scripts = JSTransform
        .getScripts(gcsTransformFns);
    Assert.assertEquals(4, scripts.size());

    List<String> goodScripts = JSTransform
        .getScripts(gcsTransformFns + "/good_script/");
    Assert.assertEquals(1, goodScripts.size());
  }


  @Test
  public void testJSTransform_getInvocable() throws ScriptException, NoSuchMethodException {

    Invocable invocable = JSTransform.buildInvocable(
        gcsTransformFns + "/good_script/");
    String output = (String) invocable.invokeFunction("transform", "{\"key\": \"value\"}");
    String expected = "{\"Some Property\":\"Some Key\",\"entity jsonified\":\"{\\\"key\\\": \\\"value\\\"}\"}";
    Assert.assertEquals(expected, output);
  }
}
