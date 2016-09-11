package com.anuragkh;

import com.datastax.driver.core.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class CassandraBenchmark {

  private Cluster cluster;
  private String datasetName;
  private int numAttributes;
  private boolean disableCompression;
  private String dataPath;
  private String insertPath;
  private String filterPath;
  private AtomicLong currentKey;
  private Random rng;
  private Logger LOG = Logger.getLogger(CassandraBenchmark.class.getName());

  // Constants
  static final int WARMUP_COUNT = 1000;
  static final int MEASURE_COUNT = 10000;

  static final int THREAD_QUERY_COUNT = 75000;

  static final long WARMUP_TIME = 30000;
  static final long MEASURE_TIME = 120000;
  static final long COOLDOWN_TIME = 30000;

  public CassandraBenchmark(String hostname, String datasetName, int numAttributes,
    boolean disableCompression, String dataPath, boolean enableLoading) {

    this.datasetName = datasetName;
    this.numAttributes = numAttributes;
    this.disableCompression = disableCompression;
    this.dataPath = dataPath;
    this.insertPath = dataPath + ".inserts";
    this.filterPath = dataPath + ".queries";
    this.rng = new Random();

    LOG.info("Creating cluster builder...");
    cluster = Cluster.builder().addContactPoint(hostname)
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000)).build();

    createKeyspace();

    createTable();

    this.currentKey = new AtomicLong(countRows());
    if (enableLoading) {
      loadData();
    } else {
      LOG.info("Skipping data loading as instructed...");
    }

    LOG.info("Initialization complete.");
  }

  private long countRows() {
    Session session = cluster.connect("bench");
    Row result = session.execute("SELECT COUNT(*) as cnt FROM " + datasetName + ";").one();
    session.close();
    return result.getLong("cnt");
  }

  private void truncateTable() {
    LOG.info("Truncating table " + datasetName);
    Session session = cluster.connect("bench");
    session.execute("TRUNCATE " + datasetName + ";");
    session.close();
    LOG.info("Table truncated.");
  }

  private void createKeyspace() {
    // Create `bench` keyspace if it does not exist
    LOG.info("Attempting to create `bench` keyspace (if it does not exist)...");
    Session session = cluster.connect();
    session.execute("CREATE KEYSPACE IF NOT EXISTS bench WITH "
      + "replication = {'class':'SimpleStrategy','replication_factor':1}");
    session.close();
  }

  private void createTable() {
    LOG.info("Creating table " + datasetName + " (if it does not exist)...");
    Session session = cluster.connect("bench");

    // Create table if it does not exist
    String createStmt = "CREATE TABLE IF NOT EXISTS " + datasetName + "(key BIGINT, ";
    for (int i = 0; i < numAttributes; i++) {
      createStmt += "field" + i + " VARCHAR, ";
    }
    createStmt += "PRIMARY KEY(key)) ";
    if (disableCompression) {
      createStmt += "WITH compression = { 'sstable_compression' : '' }";
    }
    createStmt += ";";
    session.execute(createStmt);

    // Create index on each attribute
    for (int i = 0; i < numAttributes; i++) {
      LOG.info("Creating index on field" + i + " (if it does not exist)...");
      session.execute("CREATE INDEX IF NOT EXISTS ON " + datasetName + "(\"field" + i + "\");");
    }

    session.close();
  }

  private String insertStatement(String line) {
    String[] values = line.split("\\|");
    if (values.length < numAttributes) {
      throw new IllegalArgumentException(line + " has < " + numAttributes + " attributes.");
    }
    String insertStmt = "INSERT INTO " + datasetName + " (";
    for (int i = 0; i < numAttributes; i++) {
      insertStmt += "field" + i + ", ";
    }
    insertStmt += "key) values (";
    for (int i = 0; i < numAttributes; i++) {
      insertStmt += "\'" + values[i] + "\', ";
    }
    insertStmt += currentKey.getAndAdd(1) + ");";
    return insertStmt;
  }

  private String getStatement(long key) {
    return "SELECT * FROM " + datasetName + " WHERE key = " + key + ";";
  }

  private String deleteStatement(long key) {
    return "DELETE FROM " + datasetName + " WHERE key = " + key + ";";
  }

  private String filterStatement(int fieldId, String fieldValue) {
    return "SELECT * FROM " + datasetName + " WHERE field" + fieldId + " = \'" + fieldValue + "\';";
  }

  private void loadData() {
    Session session = cluster.connect("bench");
    truncateTable();

    try (BufferedReader br = new BufferedReader(new FileReader(dataPath))) {
      String line;
      LOG.info("Loading data...");
      while ((line = br.readLine()) != null) {
        session.execute(insertStatement(line));
        if (currentKey.get() % 100000 == 0) {
          LOG.info("Loaded " + currentKey.get() + " keys.");
        }
      }
    } catch (FileNotFoundException e) {
      LOG.severe("File " + dataPath + " not found: " + e.getMessage());
      System.exit(0);
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
    LOG.info("Load finished: Inserted " + currentKey.get() + " records.");
  }

  private void benchmarkLatency(String outputPath, ArrayList<String> queries) {
    Session session = cluster.connect("bench");

    // Warmup
    int warmupCount = queries.size() / 11;
    int measureCount = queries.size() - warmupCount;
    LOG.info("Warming up for " + warmupCount + " queries...");
    for (int i = 0; i < warmupCount; i++) {
      session.execute(queries.get(i));
    }
    LOG.info("Warmup complete.");

    // Measure
    LOG.info("Measuring for " + measureCount + " queries...");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
      for (int i = warmupCount; i < warmupCount + measureCount; i++) {
        long startTime = System.nanoTime();
        ResultSet resultSet = session.execute(queries.get(i));
        int count = resultSet.all().size();
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        bw.write(count + "\t" + duration + "\n");
      }
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
  }

  private ArrayList<String> loadGetQueries(int numQueries) {
    // Generate queries
    LOG.info("Generating get queries...");
    ArrayList<String> queries = new ArrayList<>();
    for (int i = 0; i < numQueries; i++) {
      long key = rng.nextLong() % currentKey.get();
      queries.add(getStatement(key));
    }
    Collections.shuffle(queries);
    LOG.info("Generated queries.");
    return queries;
  }

  private ArrayList<String> loadInsertQueries(int numQueries) {
    // Generate queries
    LOG.info("Generating insert queries...");
    ArrayList<String> queries = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(insertPath))) {
      String line;
      while ((line = br.readLine()) != null && queries.size() < numQueries) {
        queries.add(insertStatement(line));
      }
    } catch (FileNotFoundException e) {
      LOG.severe("File " + insertPath + " not found: " + e.getMessage());
      System.exit(0);
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
    Collections.shuffle(queries);
    LOG.info("Generated queries.");
    return queries;
  }

  private ArrayList<String> loadDeleteQueries(int numQueries) {
    // Generate queries
    LOG.info("Generating delete queries...");
    ArrayList<String> queries = new ArrayList<>();
    for (int i = 0; i < numQueries; i++) {
      long key = rng.nextLong() % currentKey.get();
      queries.add(deleteStatement(key));
    }
    Collections.shuffle(queries);
    LOG.info("Generated queries.");
    return queries;
  }

  private ArrayList<String> loadFilterQueries(int numQueries) {
    // Generate queries
    LOG.info("Generating insert queries...");
    ArrayList<String> queries = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(filterPath))) {
      String line;
      while ((line = br.readLine()) != null && queries.size() < numQueries) {
        Scanner lineScanner = new Scanner(line);
        int fieldId = lineScanner.nextInt();
        String fieldValue = lineScanner.next();
        queries.add(filterStatement(fieldId, fieldValue));
      }
    } catch (FileNotFoundException e) {
      LOG.severe("File " + filterPath + " not found: " + e.getMessage());
      System.exit(0);
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
    Collections.shuffle(queries);
    LOG.info("Generated queries.");
    return queries;
  }

  public void benchmarkGetLatency() {
    benchmarkLatency("get_latency", loadGetQueries(WARMUP_COUNT + MEASURE_COUNT));
  }

  public void benchmarkInsertLatency() {

    benchmarkLatency("insert_latency", loadInsertQueries(WARMUP_COUNT + MEASURE_COUNT));
  }

  public void benchmarkDeleteLatency() {
    benchmarkLatency("delete_latency", loadDeleteQueries(WARMUP_COUNT + MEASURE_COUNT));
  }

  public void benchmarkFilterLatency() {
    benchmarkLatency("filter_latency", loadFilterQueries(WARMUP_COUNT + MEASURE_COUNT));
  }

  private ArrayList<String> threadQueries(double getM, double insertM, double deleteM,
    double filterM) throws IOException {

    Iterator<String> getQueries = loadGetQueries(THREAD_QUERY_COUNT).iterator();
    Iterator<String> insertQueries = loadInsertQueries(THREAD_QUERY_COUNT).iterator();
    Iterator<String> deleteQueries = loadDeleteQueries(THREAD_QUERY_COUNT).iterator();
    Iterator<String> filterQueries = loadFilterQueries(THREAD_QUERY_COUNT).iterator();
    ArrayList<String> queries = new ArrayList<>();

    Random localRng = new Random();
    for (int i = 0; i < THREAD_QUERY_COUNT; i++) {
      double r = localRng.nextDouble();
      if (r <= getM) {
        queries.add(getQueries.next());
      } else if (r <= insertM) {
        queries.add(insertQueries.next());
      } else if (r <= deleteM) {
        queries.add(deleteQueries.next());
      } else if (r <= filterM) {
        queries.add(filterQueries.next());
      }
    }

    return queries;
  }

  class BenchmarkThread extends Thread {
    private int index;
    private Iterator<String> queries;
    private Session session;
    private double queryThput;
    private double keyThput;

    public BenchmarkThread(int index, ArrayList<String> queries) {
      this.index = index;
      this.queries = queries.iterator();
      this.session = cluster.connect("bench");
      this.keyThput = 0.0;
      this.queryThput = 0.0;
    }

    public int getIndex() {
      return index;
    }

    public double getKeyThput() {
      return keyThput;
    }

    public double getQueryThput() {
      return queryThput;
    }

    private int executeOne() {
      ResultSet resultSet = session.execute(queries.next());
      return resultSet.all().size();
    }

    @Override public void run() {
      // Warmup
      long warmupStart = System.currentTimeMillis();
      while (System.currentTimeMillis() - warmupStart < WARMUP_TIME && queries.hasNext()) {
        executeOne();
      }

      if (!queries.hasNext()) {
        LOG.severe("Ran out of queries in warmup phase.");
        return;
      }

      // Measure
      long numQueries = 0;
      long numKeys = 0;
      long measureStart = System.currentTimeMillis();
      while (System.currentTimeMillis() - measureStart < MEASURE_TIME && queries.hasNext()) {
        numKeys += executeOne();
        numQueries++;
      }
      long measureEnd = System.currentTimeMillis();
      double totsecs = (double) (measureEnd - measureStart) / 1000.0;
      queryThput = (double) numQueries / totsecs;
      keyThput = (double) numKeys / totsecs;

      if (!queries.hasNext()) {
        LOG.warning("Ran out of queries in measure phase.");
        return;
      }

      // Cooldown
      long cooldownStart = System.currentTimeMillis();
      while (System.currentTimeMillis() - cooldownStart < COOLDOWN_TIME && queries.hasNext()) {
        executeOne();
      }
    }
  }

  public void benchmarkThroughput(double getFrac, double insertFrac, double deleteFrac,
    double filterFrac, int numThreads) {
    double insertM = getFrac + insertFrac;
    double deleteM = insertM + deleteFrac;
    double filterM = deleteM + filterFrac;
    if (filterM != 1.0) {
      LOG.warning("Expected filter mark to be 1.0, but is actually " + filterM);
    }

    BenchmarkThread[] threads = new BenchmarkThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i] = new BenchmarkThread(i, threadQueries(getFrac, insertM, deleteM, filterM));
      } catch (IOException e) {
        LOG.severe("Error in loading queries for thread " + i);
        System.exit(0);
      }
    }

    for (BenchmarkThread thread : threads) {
      thread.start();
    }

    String resFile =
      "throughput_" + getFrac + "_" + insertFrac + "_" + deleteFrac + "_" + filterFrac + "_"
        + numThreads;
    try (BufferedWriter br = new BufferedWriter(new FileWriter(resFile))) {
      for (BenchmarkThread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.severe("Thread " + thread.getIndex() + " was interrupted: " + e.getMessage());
        }
        br.write(thread.getKeyThput() + "\t" + thread.getQueryThput() + "\n");
      }
    } catch (IOException e) {
      LOG.severe("I/O exception writing to output file: " + e.getMessage());
    }
  }

  public void close() {
    cluster.close();
  }
}
