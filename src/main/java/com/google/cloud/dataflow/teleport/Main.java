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

import javax.script.ScriptException;
import java.util.Arrays;
import java.io.IOException;

/**
 * Created by bookman on 5/18/17.
 */
public class Main {
  private enum Pipeline {
    DATASTORE_TO_GCS("datastore_to_gcs"),
    GCS_TO_DATASTORE("gcs_to_datastore");

    private String mArgument;

    Pipeline(String argument) {
      mArgument = argument;
    }

    public String getArgument() { return mArgument; }

    public static Pipeline getPipeline(String argument) {
      for (Pipeline p : Pipeline.values()) {
        if (p.getArgument().equals(argument)) {
          return p;
        }
      }
      return null;
    }
  }

  public static void main(String[] cliargs)
      throws IllegalAccessException, InstantiationException, IOException, ScriptException {
    if (cliargs.length < 1) {
      throw new IllegalArgumentException(help("No pipeline specified"));
    }

    String[] pipelineArgs = Arrays.copyOfRange(cliargs, 1, cliargs.length);
    switch (Pipeline.getPipeline(cliargs[0])) {
      case DATASTORE_TO_GCS:
        DatastoreToGcs.main(pipelineArgs);
        break;
      case GCS_TO_DATASTORE:
        GcsToDatastore.main(pipelineArgs);
      default:
        help("Pipeline specified does not exist");
        break;
    }
  }

  public static String help(String msg) {
    String usage = "java -jar teleport.jar [pipeline] ...";
    String pipelines = "";
    for (Pipeline p : Pipeline.values()) {
      pipelines += "\t\t" + p.getArgument() + "\n";
    }


    return String.format("\n%s\n%s\n%s\n%s",
      msg, usage, "\tWhere [pipeline] is one of::", pipelines);
  }
}
