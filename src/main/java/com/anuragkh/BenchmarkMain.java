package com.anuragkh;

import org.apache.commons.cli.*;

import java.util.logging.Logger;

public class BenchmarkMain {
  public static void main(String[] args) {
    String logFormat = "%1$tF %1$tT %4$s %2$s %5$s%6$s%n";
    System.setProperty("java.util.logging.SimpleFormatter.format", logFormat);

    Logger LOG = Logger.getLogger(BenchmarkMain.class.getName());

    Options options = new Options();

    Option benchmarkTypeOpt = new Option("b", true, "Benchmark type.");
    options.addOption(benchmarkTypeOpt);

    Option numThreadsOpt = new Option("n", true, "Number of threads.");
    options.addOption(numThreadsOpt);

    Option hostnameOpt = new Option("h", true, "Server hostnameOpt.");
    options.addOption(hostnameOpt);

    Option numAttrsOpt = new Option("a", true, "Number of attributes.");
    numAttrsOpt.setRequired(true);
    options.addOption(numAttrsOpt);

    Option datasetNameOpt = new Option("d", true, "Dataset name.");
    options.addOption(datasetNameOpt);

    Option enableCompressionOpt = new Option("c", false, "Enable compression.");
    options.addOption(enableCompressionOpt);

    Option inputDataPathOpt = new Option("i", true, "Input data path.");
    inputDataPathOpt.setRequired(true);
    options.addOption(inputDataPathOpt);

    Option loadDataOpt = new Option("l", false, "Execute load phase.");
    options.addOption(loadDataOpt);

    Option benchOpt = new Option("r", false, "Execute request phase.");
    options.addOption(benchOpt);

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

    String benchmarkType = "latency-get";
    if (cmd.hasOption('b')) {
      benchmarkType = cmd.getOptionValue('b');
    }

    int numThreads = 1;
    if (cmd.hasOption('n')) {
      numThreads = Integer.parseInt(cmd.getOptionValue('n'));
    }

    String hostname = "localhost";
    if (cmd.hasOption('h')) {
      hostname = cmd.getOptionValue('h');
    }

    int numAttrs = Integer.parseInt(cmd.getOptionValue('a'));

    String datasetName = "test";
    if (cmd.hasOption('d')) {
      datasetName = cmd.getOptionValue('d');
    }

    String dataPath = cmd.getOptionValue('i');
    boolean disableComp = !cmd.hasOption('c');
    boolean enableLoad = cmd.hasOption('l');
    boolean enableRequest = cmd.hasOption('r');

    CassandraBenchmark benchmark =
      new CassandraBenchmark(hostname, datasetName, numAttrs, disableComp, dataPath, enableLoad);

    if (enableRequest) {
      switch (benchmarkType) {
        case "latency-get":
          benchmark.benchmarkGetLatency();
          break;
        case "latency-insert":
          benchmark.benchmarkInsertLatency();
          break;
        case "latency-delete":
          benchmark.benchmarkDeleteLatency();
          break;
        case "latency-filter":
          benchmark.benchmarkFilterLatency();
          break;
        default:
          if (benchmarkType.startsWith("throughput")) {
            String[] parts = benchmarkType.split("\\-");
            if (parts.length != 5) {
              benchmark.close();
              System.out.println("Invalid benchmarkType: " + benchmarkType);
              formatter.printHelp("cassandra-bench", options);
              break;
            }
            double getFrac = Double.parseDouble(parts[1]);
            double insertFrac = Double.parseDouble(parts[2]);
            double deleteFrac = Double.parseDouble(parts[3]);
            double filterFrac = Double.parseDouble(parts[4]);
            benchmark.benchmarkThroughput(getFrac, insertFrac, deleteFrac, filterFrac, numThreads);
          } else {
            System.out.println("Invalid benchmarkType: " + benchmarkType);
            formatter.printHelp("cassandra-bench", options);
          }
      }
    } else {
      LOG.info("Skipping benchmark phase as instructed.");
    }

    benchmark.close();
  }
}
