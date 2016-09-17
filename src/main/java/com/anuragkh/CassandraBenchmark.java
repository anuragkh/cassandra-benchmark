package com.anuragkh;

import com.datastax.driver.core.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class CassandraBenchmark {

  private Cluster cluster;
  private String dataPath;
  private String attrPath;
  private AtomicLong currentKey;

  private int[] timestamps;
  private String[] srcips;
  private String[] dstips;
  private int[] sports;
  private int[] dports;
  private byte[][] datas;

  private Logger LOG = Logger.getLogger(CassandraBenchmark.class.getName());

  static final long REPORT_RECORD_INTERVAL = 10000;
  static final String TABLE_NAME = "packets";
  static final String SCHEMA =
    "(id BIGINT PRIMARY KEY, ts INT, srcip VARCHAR, dstip VARCHAR, sport INT, dport INT, data BLOB)";

  static final String INSERT_OP =
    "INSERT INTO packets (id, ts, srcip, dstip, sport, dport, data) values (?,?,?,?,?,?,?)";

  public CassandraBenchmark(String hostname, String dataPath, String attrPath) {

    this.dataPath = dataPath;
    this.attrPath = attrPath;

    LOG.info("Creating cluster builder...");
    cluster = Cluster.builder().addContactPoint(hostname)
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000)).build();

    createKeyspace();

    createTable();

    truncateTable();

    loadData();

    this.currentKey = new AtomicLong(0L);

    LOG.info("Initialization complete.");
  }

  private void truncateTable() {
    LOG.info("Truncating table " + TABLE_NAME);
    Session session = cluster.connect("bench");
    session.execute("TRUNCATE " + TABLE_NAME + ";");
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
    LOG.info("Creating table " + TABLE_NAME + " (if it does not exist)...");
    Session session = cluster.connect("bench");

    // Create table if it does not exist
    String createStmt = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " " + SCHEMA + ";";
    session.execute(createStmt);

    // Create index on each attribute
    LOG.info("Creating index on ts (if it does not exist)...");
    session.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE_NAME + "(\"ts\");");

    LOG.info("Creating index on srcip (if it does not exist)...");
    session.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE_NAME + "(\"srcip\");");

    LOG.info("Creating index on dstip (if it does not exist)...");
    session.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE_NAME + "(\"dstip\");");

    LOG.info("Creating index on sport (if it does not exist)...");
    session.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE_NAME + "(\"sport\");");

    LOG.info("Creating index on dport (if it does not exist)...");
    session.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE_NAME + "(\"dport\");");

    session.close();
  }

  BoundStatement insertOp(PreparedStatement ps, long id, int ts, String sIP, String dIP, int sPort,
    int dPort, byte[] data) {
    BoundStatement bs = new BoundStatement(ps);
    return bs.bind(id, ts, sIP, dIP, sPort, dPort, ByteBuffer.wrap(data));
  }

  private int countLines() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(attrPath));
    } catch (FileNotFoundException e) {
      LOG.severe("Error: " + e.getMessage());
      System.exit(-1);
    }
    int lines = 0;
    try {
      while (reader.readLine() != null)
        lines++;
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lines;
  }

  private void loadData() {
    // Generate queries
    LOG.info("Loading packet data into memory...");

    BufferedInputStream dataStream;
    BufferedReader attrReader;

    // Allocate space for packet data
    int numPackets = countLines();
    timestamps = new int[numPackets];
    srcips = new String[numPackets];
    dstips = new String[numPackets];
    sports = new int[numPackets];
    dports = new int[numPackets];
    datas = new byte[numPackets][];

    try {
      dataStream = new BufferedInputStream(new FileInputStream(dataPath));
      attrReader = new BufferedReader(new FileReader(attrPath));
      String attrLine;
      int i = 0;
      while ((attrLine = attrReader.readLine()) != null) {
        String[] attrs = attrLine.split("\\s+");
        if (attrs.length != 6) {
          LOG.severe("Error parsing attribute line: " + attrLine);
          System.exit(-1);
        }
        timestamps[i] = Integer.parseInt(attrs[0]);
        int length = Integer.parseInt(attrs[1]);
        srcips[i] = attrs[2];
        dstips[i] = attrs[3];
        sports[i] = Integer.parseInt(attrs[4]);
        dports[i] = Integer.parseInt(attrs[5]);
        datas[i] = new byte[length];
        int nbytes = dataStream.read(datas[i]);
        if (nbytes != length) {
          LOG.severe("Error reading data: Length " + length + " does not match num bytes read.");
          System.exit(-1);
        }
        i++;
      }
    } catch (FileNotFoundException e) {
      LOG.severe("File not found: " + e.getMessage());
      System.exit(0);
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
    LOG.info("Loaded packet data in memory.");
  }

  class ProgressLogger {
    private BufferedWriter out;

    public ProgressLogger(String fileName) {
      try {
        out = new BufferedWriter(new FileWriter(fileName));
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }

    public synchronized void logProgress(long numOps) {
      try {
        out.write(System.currentTimeMillis() + " " + numOps + "\n");
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }

    public void close() {
      try {
        out.close();
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }
  }


  class LoaderThread extends Thread {
    private int index;
    private long timebound;
    private Session session;
    private int localOpsProcessed;
    private double throughput;
    private PreparedStatement ps;
    private ProgressLogger logger;

    public LoaderThread(int index, long timebound, ProgressLogger logger) {
      this.index = index;
      this.timebound = timebound * 1000;
      this.session = cluster.connect("bench");
      this.ps = session.prepare(INSERT_OP);
      this.logger = logger;
      this.localOpsProcessed = 0;
      this.throughput = 0.0;
    }

    public int getIndex() {
      return index;
    }

    public double getThroughput() {
      return throughput;
    }

    private int executeOne() {
      long id = currentKey.getAndAdd(1L);
      int i = (int) id;
      if (i >= timestamps.length)
        return -1;
      BoundStatement bs =
        insertOp(ps, id, timestamps[i], srcips[i], dstips[i], sports[i], dports[i], datas[i]);
      session.execute(bs);
      localOpsProcessed++;
      return i + 1;
    }

    @Override public void run() {
      int totOpsProcessed = 0;
      long measureStart = System.currentTimeMillis();
      while (totOpsProcessed != -1 && System.currentTimeMillis() - measureStart < timebound) {
        totOpsProcessed = executeOne();
        if (totOpsProcessed % REPORT_RECORD_INTERVAL == 0) {
          logger.logProgress(totOpsProcessed);
        }
      }
      long measureEnd = System.currentTimeMillis();
      double totsecs = (double) (measureEnd - measureStart) / 1000.0;
      throughput = (double) localOpsProcessed / totsecs;
    }
  }

  public void loadPackets(int numThreads, long timebound) {

    LoaderThread[] threads = new LoaderThread[numThreads];
    ProgressLogger logger = new ProgressLogger("record_progress");

    for (int i = 0; i < numThreads; i++) {
      LOG.info("Initializing thread " + i + "...");
      threads[i] = new LoaderThread(i, timebound, logger);
      LOG.info("Thread " + i + " initialization complete.");
    }

    long startTime = System.currentTimeMillis();
    for (LoaderThread thread : threads) {
      thread.start();
    }

    String resFile = "write_throughput";
    try (BufferedWriter br = new BufferedWriter(new FileWriter(resFile))) {
      for (LoaderThread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.severe("Thread " + thread.getIndex() + " was interrupted: " + e.getMessage());
        }
        br.write(thread.getThroughput() + "\n");
      }
    } catch (IOException e) {
      LOG.severe("I/O exception writing to output file: " + e.getMessage());
    }
    long endTime = System.currentTimeMillis();

    LOG.info("Finished loading packets in " + (endTime - startTime) / 1000 + "s");

    logger.close();
  }

  public void close() {
    cluster.close();
  }
}
