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

package com.google.cloud.dataflow.teleport.Helpers;

import com.google.api.gax.paging.Page;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Handles all Javascript Transform related aspects
 */
@AutoValue
public abstract class JSTransform {
  @Nullable abstract String gcsJSPath();
  abstract String engineName();
  abstract Optional<String> project();
  private static Invocable mInvocable;

  public static Builder newBuilder() {
    return new com.google.cloud.dataflow.teleport.Helpers.AutoValue_JSTransform.Builder()
        .setEngineName("JavaScript")
        .setGcsJSPath("");
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setGcsJSPath(String gcsJSPath);
    public abstract Builder setEngineName(String engineName);
    public abstract Builder setProject(Optional<String> project);
    public abstract JSTransform build();
  }

  private Storage getStorageService() {
    StorageOptions storageOptions = StorageOptions.getDefaultInstance();
    if (project().isPresent()) {
      storageOptions = StorageOptions.newBuilder()
          .setProjectId(project().get())
          .build();
    }
    return storageOptions.getService();
  }

  public List<String> getScripts() {
    if (Strings.isNullOrEmpty(gcsJSPath())) {
      return new ArrayList<>();
    }

    String bucketName = gcsJSPath().replace("gs://", "").split("/")[0];
    String prefixPath = gcsJSPath().replace("gs://" + bucketName + "/", "");

    Bucket bucket = getStorageService().get(bucketName);

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

  public String invoke(String data) throws ScriptException, NoSuchMethodException {
    return (String) getInvocable().invokeFunction("transform", data);
  }


  public boolean hasTransform() throws ScriptException {
    return (getInvocable() != null);
  }

  @Nullable
  public Invocable getInvocable() throws ScriptException {
    if (Strings.isNullOrEmpty(gcsJSPath())) {
      return null;
    }

    if (mInvocable == null) {
      ScriptEngineManager engineManager = new ScriptEngineManager();
      ScriptEngine scriptEngine = engineManager.getEngineByName(engineName());

      for (String script : getScripts()) {
        scriptEngine.eval(script);
      }

      mInvocable = (Invocable) scriptEngine;
    }
    return mInvocable;
  }
}
