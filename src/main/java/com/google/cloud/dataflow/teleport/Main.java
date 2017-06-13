package com.google.cloud.dataflow.teleport;

import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Created by bookman on 5/18/17.
 */
public class Main {
  private enum Pipeline {
    DATASTORE_TO_GCS("datastore_to_gcs");

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
    System.out.println("Pipeline Args");
    for(String s : pipelineArgs) {
      System.out.println(s);
    }

    Pipeline p = Pipeline.getPipeline(cliargs[0]);
    switch (p) {
      case DATASTORE_TO_GCS:
        DatastoreToGcs.main(pipelineArgs);
        break;
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
