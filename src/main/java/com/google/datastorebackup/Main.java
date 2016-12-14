package com.google.datastorebackup;


public class Main {
  public static void main(String[] args) throws Exception {
    // remove our pseudo operation
    String[] operationArgs = new String[args.length - 1];
    for (int i = 1; i < args.length; ++i) {
      operationArgs[i-1] = args[i];
    }

    if (args[0].equals("gcsbackup")) {
      GCSBackup.run(operationArgs);
    } else if (args[0].equals("gcsrestore")) {
      GCSRestore.run(operationArgs);
    } else if (args[0].equals("bqbackup")) {
      new BQBackup().run(operationArgs);
    } else {
      System.err.println("Unknown operation.");
    }
  }
}
