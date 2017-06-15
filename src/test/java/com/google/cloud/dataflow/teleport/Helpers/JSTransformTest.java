package com.google.cloud.dataflow.teleport.Helpers;

import com.google.common.base.Strings;
import java.util.List;
import javax.script.Invocable;
import javax.script.ScriptException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the JSTransform Class
 */
public class JSTransformTest {
  public static final String gcsTransformFns = "gs://teleport-test/test/transforms/";
  public static final String goodGcsTransform = "gs://teleport-test/test/transforms/good";
  public static final String badGcsTransform = "gs://teleport-test/test/transforms/errors";

  @Test
  public void testJSTransform_getScripts() {
    // Listing just javascript files folder gets all scripts
    JSTransform goodTransforms = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .build();

    Assert.assertEquals(2, goodTransforms.getScripts().size());


    // Listing a single js script gets it
    JSTransform singleTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform + "/transform.js")
        .build();

    Assert.assertEquals(1, singleTransform.getScripts().size());


    // Listing a directory with more than just js gets just js
    JSTransform folderTransforms = JSTransform.newBuilder()
        .setGcsJSPath(gcsTransformFns)
        .build();

    Assert.assertEquals(4, folderTransforms.getScripts().size());

    // Transform Fns are non null strings
    for (String s : folderTransforms.getScripts()) {
      Assert.assertFalse(Strings.isNullOrEmpty(s));
    }
  }


  @Test
  public void testJSTransform_invoke() throws ScriptException, NoSuchMethodException {
    // Test JS Transform involving multiple files
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .build();

    String output = (String) jsTransform.invoke( "{\"key\": \"value\"}");
    String expected = "{\"Some Property\":\"Some Key\",\"entity jsonified\":\"{\\\"key\\\": \\\"value\\\"}\"}";
    Assert.assertEquals(expected, output);
  }

  public void testJSTransform_getInvocable() throws ScriptException {
    // Test a good invocable
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath(goodGcsTransform)
        .build();
    Invocable invocable = jsTransform.getInvocable();

    // Test an invocable that should throw an exception
    ScriptException scriptException = null;
    try {
      JSTransform.newBuilder()
          .setGcsJSPath(badGcsTransform)
          .build()
          .getInvocable();
    } catch (ScriptException e) {
      scriptException = e;
    }
    Assert.assertNotNull(scriptException);
  }

  @Test
  public void testJSTransform_hasTransform() throws ScriptException {
    JSTransform jsTransform = JSTransform.newBuilder()
        .setGcsJSPath("")
        .build();
    Assert.assertFalse(jsTransform.hasTransform());
  }
}
