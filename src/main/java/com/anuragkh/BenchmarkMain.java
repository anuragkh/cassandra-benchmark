package com.anuragkh;

import org.apache.commons.cli.*;

public class BenchmarkMain {
  public static void main(String[] args) {
    String logFormat = "%1$tF %1$tT %4$s %2$s %5$s%6$s%n";
    System.setProperty("java.util.logging.SimpleFormatter.format", logFormat);

    Options options = new Options();

    Option numThreadsOpt = new Option("n", true, "Number of threads.");
    options.addOption(numThreadsOpt);

    Option hostnameOpt = new Option("h", true, "Server hostname.");
    options.addOption(hostnameOpt);

    Option numAttrsOpt = new Option("a", true, "Attribute Path.");
    numAttrsOpt.setRequired(true);
    options.addOption(numAttrsOpt);

    Option inputDataPathOpt = new Option("i", true, "Data path.");
    inputDataPathOpt.setRequired(true);
    options.addOption(inputDataPathOpt);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("cassandra-bench", options);

      return;
    }

    int numThreads = 1;
    if (cmd.hasOption('n')) {
      numThreads = Integer.parseInt(cmd.getOptionValue('n'));
    }

    String hostname = "localhost";
    if (cmd.hasOption('h')) {
      hostname = cmd.getOptionValue('h');
    }

    String attrPath = cmd.getOptionValue('a');
    String dataPath = cmd.getOptionValue('i');

    CassandraBenchmark benchmark = new CassandraBenchmark(hostname, dataPath, attrPath);
    benchmark.loadPackets(numThreads);

    benchmark.close();
  }
}
