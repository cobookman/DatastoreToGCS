package com.google.datastorebackup;


public class Main {
  public static void main(String[] args) {
    // remove our pseudo operation
    String[] operationArgs = new String[args.length - 1];
    for (int i = 1; i < args.length; ++i) {
      operationArgs[i-1] = args[i];
    }

    if (args[0].equals("backup")) {
      Backup.run(operationArgs);
    } else if (args[0].equals("restore")) {
      Restore.run(operationArgs);
    } else {
      System.err.println("Unknown operation.");
    }
  }
}
