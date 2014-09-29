import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.stats.Snapshot;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.zookeeper.server.persistence.SnapShot;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class DFSBenchmark {

  ListeningExecutorService service;
  static final MetricsRegistry metrics = new MetricsRegistry();
  static final Histogram responseTimeHistogram = metrics.newHistogram(DFSBenchmark.class, "ResponseTime_Historgram");
  static final Counter fileOpens = Metrics.newCounter(DFSBenchmark.class, "file-opened");
  static final ConsoleReporter reporter = new ConsoleReporter(System.out);
  static final Meter throughput = Metrics.newMeter(DFSBenchmark.class, "throughput/sec", "", TimeUnit.SECONDS);

  private int threads;
  private Path baseDirPath;
  private List<ListenableFuture<Long>> futureList = Lists.newLinkedList();
  private static CountDownLatch greenSignal;

  private static final Log LOG = LogFactory.getLog(DFSBenchmark.class);

  public DFSBenchmark(int threads, Path basePath) {
    this.threads = threads;
    this.baseDirPath = basePath;
    greenSignal = new CountDownLatch(threads);
    service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
    reporter.enable(metrics, 3000, TimeUnit.MILLISECONDS);
  }

  public void runBenchmark() throws ExecutionException, InterruptedException, IOException {
    for (int i = 0; i < threads; i++) {
      futureList.add(service.submit(new Worker(baseDirPath)));
    }
    LOG.info("Submitted the tasks..running..");

    for (ListenableFuture<Long> future : futureList) {
      responseTimeHistogram.update(future.get().longValue());
    }

    service.shutdown();

    System.out.println("Total number of fileOpens : " + fileOpens.count());
    System.out.println("Response time histogram");
    reporter.processHistogram(new MetricName("", "", "ResponseTime"), responseTimeHistogram, System.out);
    reporter.processMeter(new MetricName("", "", "ThroughPut per second"), throughput, System.out);
    metrics.shutdown();

    LOG.info("Done!!..");
  }

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2, "Please provide the threads and base directory path details");
    DFSBenchmark benchmark = new DFSBenchmark((Integer.parseInt(args[0])), new Path(args[1]));
    benchmark.runBenchmark();
  }

  /**
   * Simple worker which would just list all the files from base directory
   * and do a file open/close.  No reads would be performed.
   */
  static class Worker implements Callable {
    FileSystem fs;
    Configuration conf;
    RemoteIterator<LocatedFileStatus> filePaths;

    public Worker(Path path) throws IOException {
      conf = new Configuration();
      fs = FileSystem.get(conf);
      LOG.info("Got FS..");
      greenSignal.countDown();
      filePaths = fs.listFiles(path, true);
      LOG.info("Got the set of files to be opened/closed");
    }

    @Override public Long call() throws IOException, InterruptedException {
      greenSignal.await(); //wait till all threads are ready (i.e obtaining fs etc)
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        while (filePaths.hasNext()) {
          Path filePath = filePaths.next().getPath();
          FSDataInputStream in = fs.open(filePath);
          in.read();
          in.close();
          fileOpens.inc();
          throughput.mark();
        }
        return stopwatch.stop().elapsedTime(TimeUnit.MILLISECONDS);
      } catch (IOException e) {
        LOG.error("Some io exception", e);
        throw e;
      } finally {
        //fs.close(); //If we enable this, it would end up throwing exceptions in other threads (as internally it would have closed the FS).
      }
    }
  }
}
