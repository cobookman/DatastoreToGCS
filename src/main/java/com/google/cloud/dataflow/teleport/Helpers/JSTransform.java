package com.google.cloud.dataflow.teleport.Helpers;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Handles all Javascript Transform related aspects
 */
public class JSTransform {
  public static List<String> getScripts(String gcsTransformFn) {

    Storage storage = StorageOptions.getDefaultInstance().getService();
    String bucketName = gcsTransformFn.replace("gs://", "").split("/")[0];
    String prefixPath = gcsTransformFn.replace("gs://" + bucketName + "/", "");

    Bucket bucket = storage.get(bucketName);

    ArrayList<String> filePaths = new ArrayList<>();
    if (prefixPath.endsWith(".js")) {
      filePaths.add(prefixPath);
    } else {
      Page<Blob> blobs = bucket.list(BlobListOption.prefix(prefixPath));
      blobs.iterateAll().forEach((Blob blob) -> {
        if (blob.getName().endsWith(".js")) {
          filePaths.add(blob.getName());
        }
      });
    }

    List<String> scripts = new ArrayList<>();

    for (String filePath : filePaths) {
      scripts.add(new String(bucket.get(filePath).getContent()));
    }
    return scripts;
  }

  @Nullable
  public static Invocable buildInvocable(String gcsTransformFn) throws ScriptException {
    if (Strings.isNullOrEmpty(gcsTransformFn)) {
      return null;
    }

    ScriptEngineManager engineManager = new ScriptEngineManager();
    ScriptEngine scriptEngine = engineManager.getEngineByName("JavaScript");

    List<String> scripts = getScripts(gcsTransformFn);

    for (String script : scripts) {
      scriptEngine.eval(script);
    }

    return (Invocable) scriptEngine;
  }
}
