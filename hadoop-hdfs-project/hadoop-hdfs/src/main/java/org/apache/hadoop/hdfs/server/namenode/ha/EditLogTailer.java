/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.SecurityUtil;

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;


/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Log LOG = LogFactory.getLog(EditLogTailer.class);

  /**
   * StandbyNode will hold namesystem lock to apply at most this many journal
   * transactions.
   * It will then release the lock and re-acquire it to load more transactions.
   * By default the write lock is held for the entire journal segment.
   * Fine-grained locking allows read requests to get through.
   */
  public static final String  DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY =
      "dfs.ha.tail-edits.max-txns-per-lock";
  public static final long DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT =
      Long.MAX_VALUE;

  // 编辑日志跟踪线程EditLogTailerThread实例tailerThread
  private final EditLogTailerThread tailerThread;
  // HDFS配置信息Configuration实例conf
  private final Configuration conf;
  // 文件系统命名空间FSNamesystem实例namesystem
  private final FSNamesystem namesystem;
  private final Iterator<RemoteNameNodeInfo> nnLookup;
  // 文件系统编辑日志FSEditLog实例editLog
  private FSEditLog editLog;
  // Active NameNode地址InetSocketAddress
  private RemoteNameNodeInfo currentNN;

  /**
   * The last transaction ID at which an edit log roll was initiated.
   * 一次编辑日志滚动开始时的最新事务ID
   */
  private long lastRollTriggerTxId = HdfsServerConstants.INVALID_TXID;
  
  /**
   * The highest transaction ID loaded by the Standby.
   * StandBy NameNode加载的最高事务ID
   */
  private long lastLoadedTxnId = HdfsServerConstants.INVALID_TXID;

  /**
   * The last time we successfully loaded a non-zero number of edits from the
   * shared directory.
   * 最后一次我们从共享目录成功加载一个非零编辑的时间
   */
  private long lastLoadTimeMs;

  /**
   * How often the Standby should roll edit logs. Since the Standby only reads
   * from finalized log segments, the Standby will only be as up-to-date as how
   * often the logs are rolled.
   * StandBy NameNode滚动编辑日志的时间间隔。
   */
  private final long logRollPeriodMs;

  /**
   * The timeout in milliseconds of calling rollEdits RPC to Active NN.
   * @see HDFS-4176.
   */
  private final long rollEditsTimeoutMs;

  /**
   * The executor to run roll edit RPC call in a daemon thread.
   */
  private final ExecutorService rollEditsRpcExecutor;

  /**
   * How often the Standby should check if there are new finalized segment(s)
   * available to be read from.
   * StandBy NameNode检查是否存在可以读取的新的最终日志段的时间间隔
   */
  private final long sleepTimeMs;

  private final int nnCount;
  private NamenodeProtocol cachedActiveProxy = null;
  // count of the number of NNs we have attempted in the current lookup loop
  private int nnLoopCount = 0;

  /**
   * maximum number of retries we should give each of the remote namenodes before giving up
   */
  private int maxRetries;

  /**
   * Whether the tailer should tail the in-progress edit log segments.
   */
  private final boolean inProgressOk;

  /**
   * Release the namesystem lock after loading this many transactions.
   * Then re-acquire the lock to load more edits.
   */
  private final long maxTxnsPerLock;

  public EditLogTailer(FSNamesystem namesystem, Configuration conf) {
    // 实例化编辑日志追踪线程EditLogTailerThread
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;
    // 根据入参初始化配置信息conf和文件系统命名系统namesystem
    this.namesystem = namesystem;
    // 从namesystem中获取editLog
    this.editLog = namesystem.getEditLog();
    // 最新加载edit log时间lastLoadTimestamp初始化为当前时间
    lastLoadTimeMs = monotonicNow();
    // StandBy NameNode滚动编辑日志的时间间隔logRollPeriodMs
    // 取参数dfs.ha.log-roll.period，参数未配置默认为2min
    logRollPeriodMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT, TimeUnit.SECONDS) * 1000;
    List<RemoteNameNodeInfo> nns = Collections.emptyList();
    // 如果logRollPeriodMs大于等于0
    if (logRollPeriodMs >= 0) {
      try {
        // 调用getActiveNodeAddress()方法初始化Active NameNode地址activeAddr
        nns = RemoteNameNodeInfo.getRemoteNameNodes(conf);
      } catch (IOException e) {
        throw new IllegalArgumentException("Remote NameNodes not correctly configured!", e);
      }

      for (RemoteNameNodeInfo info : nns) {
        // overwrite the socket address, if we need to
        InetSocketAddress ipc = NameNode.getServiceAddress(info.getConfiguration(), true);
        // sanity check the ipc address
        Preconditions.checkArgument(ipc.getPort() > 0,
            "Active NameNode must have an IPC port configured. " + "Got address '%s'", ipc);
        info.setIpcAddress(ipc);
      }

      LOG.info("Will roll logs on active node every " +
          (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
          DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }
    // StandBy NameNode检查是否存在可以读取的新的最终日志段的时间间隔sleepTimeMs
    // 取参数dfs.ha.tail-edits.period，参数未配置默认为1min
    sleepTimeMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT, TimeUnit.SECONDS) * 1000;

    rollEditsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_DEFAULT) * 1000;

    rollEditsRpcExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true).build());

    maxRetries = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY,
      DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
    if (maxRetries <= 0) {
      LOG.error("Specified a non-positive number of retries for the number of retries for the " +
          "namenode connection when manipulating the edit log (" +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY + "), setting to default: " +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
      maxRetries = DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT;
    }

    inProgressOk = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);

    this.maxTxnsPerLock = conf.getLong(
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY,
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT);

    nnCount = nns.size();
    // setup the iterator to endlessly loop the nns
    this.nnLookup = Iterators.cycle(nns);

    LOG.debug("logRollPeriodMs=" + logRollPeriodMs +
        " sleepTime=" + sleepTimeMs);
  }

  public void start() {
    tailerThread.start();
  }
  
  public void stop() throws IOException {
    rollEditsRpcExecutor.shutdown();
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  public void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }

  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    // Important to do tailing as the login user, in case the shared
    // edits storage is implemented by a JournalManager that depends
    // on security credentials to access the logs (eg QuorumJournalManager).
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          // It is already under the full name system lock and the checkpointer
          // thread is already stopped. No need to acqure any other lock.
          doTailEdits();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return null;
      }
    });
  }
  
  @VisibleForTesting
  void doTailEdits() throws IOException, InterruptedException {
    // Write lock needs to be interruptible here because the 
    // transitionToActive RPC takes the write lock before calling
    // tailer.stop() -- so if we're not interruptible, it will
    // deadlock.
    // namesystem加写锁
    namesystem.writeLockInterruptibly();
    try {
      //todo 加载当前自己的元数据日志
      // 通过namesystem获取文件系统镜像FSImage实例image
      FSImage image = namesystem.getFSImage();
      //todo standby namenode 获取当前的元数据日志的最后一条日志的事务id是多少
      long lastTxnId = image.getLastAppliedTxId();

      if (LOG.isDebugEnabled()) {
        LOG.debug("lastTxnId: " + lastTxnId);
      }
      Collection<EditLogInputStream> streams;
      long startTime = Time.monotonicNow();
      try {
        // 从编辑日志editLog中获取编辑日志输入流集合streams，获取的输入流为最新事务ID加1之后的数据
        streams = editLog.selectInputStreams(lastTxnId + 1, 0,
            null, inProgressOk, true);
      } catch (IOException ioe) {
        // This is acceptable. If we try to tail edits in the middle of an edits
        // log roll, i.e. the last one has been finalized but the new inprogress
        // edits file hasn't been started yet.
        LOG.warn("Edits tailer failed to find any streams. Will try again " +
            "later.", ioe);
        return;
      } finally {
        NameNode.getNameNodeMetrics().addEditLogFetchTime(
            Time.monotonicNow() - startTime);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("edit streams to load from: " + streams.size());
      }
      
      // Once we have streams to load, errors encountered are legitimate cause
      // for concern, so we don't catch them here. Simple errors reading from
      // disk are ignored.
      long editsLoaded = 0;
      try {
        //todo 去journalnode加载日志
        // 调用文件系统镜像FSImage实例image的loadEdits()，
        // 利用编辑日志输入流集合streams，加载编辑日志至目标namesystem中的文件系统镜像FSImage，
        // 并获得编辑日志加载的大小editsLoaded
        editsLoaded = image.loadEdits(
            streams, namesystem, maxTxnsPerLock, null, null);
      } catch (EditLogInputException elie) {
        editsLoaded = elie.getNumEditsLoaded();
        throw elie;
      } finally {
        // 如果editsLoaded大于0
        // 最后一次我们从共享目录成功加载一个非零编辑的时间lastLoadTimestamp更新为当前时间
        if (editsLoaded > 0 || LOG.isDebugEnabled()) {
          LOG.debug(String.format("Loaded %d edits starting from txid %d ",
              editsLoaded, lastTxnId));
        }
        NameNode.getNameNodeMetrics().addNumEditLogLoaded(editsLoaded);
      }

      if (editsLoaded > 0) {
        lastLoadTimeMs = monotonicNow();
      }
      // 上次StandBy NameNode加载的最高事务ID更新为image中最新事务ID
      lastLoadedTxnId = image.getLastAppliedTxId();
    } finally {
      // namesystem去除写锁
      namesystem.writeUnlock();
    }
  }

  /**
   * @return time in msec of when we last loaded a non-zero number of edits.
   */
  public long getLastLoadTimeMs() {
    return lastLoadTimeMs;
  }

  /**
   * @return true if the configured log roll period has elapsed.
   *  StandBy NameNode滚动编辑日志的时间间隔logRollPeriodMs大于0，
   * 且最后一次我们从共享目录成功加载一个非零编辑的时间到现在的时间间隔大于logRollPeriodMs
   */
  private boolean tooLongSinceLastLoad() {
    return logRollPeriodMs >= 0 && 
      (monotonicNow() - lastLoadTimeMs) > logRollPeriodMs ;
  }

  /**
   * NameNodeProxy factory method.
   * @return a Callable to roll logs on remote NameNode.
   */
  @VisibleForTesting
  Callable<Void> getNameNodeProxy() {
    return new MultipleNameNodeProxy<Void>() {
      @Override
      protected Void doWork() throws IOException {
        cachedActiveProxy.rollEditLog();
        return null;
      }
    };
  }

  /**
   * Trigger the active node to roll its logs.
   */
  @VisibleForTesting
  void triggerActiveLogRoll() {
    LOG.info("Triggering log roll on remote NameNode");
    Future<Void> future = null;
    try {
      // 获得Active NameNode的代理，并调用其rollEditLog()方法滚动编辑日志
      future = rollEditsRpcExecutor.submit(getNameNodeProxy());
      future.get(rollEditsTimeoutMs, TimeUnit.MILLISECONDS);
      // 将上次StandBy NameNode加载的最高事务ID，即lastLoadedTxnId，赋值给上次编辑日志滚动开始时的最新事务ID，即lastRollTriggerTxId，
      // 这么做是为了方便进行日志回滚
      lastRollTriggerTxId = lastLoadedTxnId;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RemoteException) {
        IOException ioe = ((RemoteException) cause).unwrapRemoteException();
        if (ioe instanceof StandbyException) {
          LOG.info("Skipping log roll. Remote node is not in Active state: " +
              ioe.getMessage().split("\n")[0]);
          return;
        }
      }
      LOG.warn("Unable to trigger a roll of the active NN", e);
    } catch (TimeoutException e) {
      if (future != null) {
        future.cancel(true);
      }
      LOG.warn(String.format(
          "Unable to finish rolling edits in %d ms", rollEditsTimeoutMs));
    } catch (InterruptedException e) {
      LOG.warn("Unable to trigger a roll of the active NN", e);
    }
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;
    
    private EditLogTailerThread() {
      super("Edit log tailer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    @Override
    public void run() {
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            //todo 重要代码
            doWork();
            return null;
          }
        });
    }
    
    private void doWork() {
      // 标志位shouldRun为true时一直循环
      while (shouldRun) {
        try {
          // There's no point in triggering a log roll if the Standby hasn't
          // read any more transactions since the last time a roll was
          // triggered.
          // 自从上次日志滚动触发以来，如果StandBy NameNode没有读到任何事务的话，没有点触发一次日志滚动，
          // 如果是自从上次加载后过了太长时间，并且上次编辑日志滚动开始时的最新事务ID小于上次StandBy NameNode加载的最高事务ID
          /**
           * 条件：
           * 1.出发的时间太久 || 2. 上次回滚id < 上次加载
           */
          if (tooLongSinceLastLoad() &&
              lastRollTriggerTxId < lastLoadedTxnId) {
            // 触发Active NameNode进行编辑日志滚动
            triggerActiveLogRoll();
          }
          /**
           * Check again in case someone calls {@link EditLogTailer#stop} while
           * we're triggering an edit log roll, since ipc.Client catches and
           * ignores {@link InterruptedException} in a few places. This fixes
           * the bug described in HDFS-2823.
           * 判断标志位shouldRun，如果其为false的话，退出循环
           */
          if (!shouldRun) {
            break;
          }
          // Prevent reading of name system while being modified. The full
          // name system lock will be acquired to further block even the block
          // state updates.
          namesystem.cpLockInterruptibly();
          long startTime = Time.monotonicNow();
          try {
            NameNode.getNameNodeMetrics().addEditLogTailInterval(
                startTime - lastLoadTimeMs);
            //todo 重要代码
            // 调用doTailEdits()方法执行日志追踪
            doTailEdits();
          } finally {
            namesystem.cpUnlock();
            NameNode.getNameNodeMetrics().addEditLogTailTime(
                Time.monotonicNow() - startTime);
          }
          //Update NameDirSize Metric
          namesystem.getFSImage().getStorage().updateNameDirSize();
        } catch (EditLogInputException elie) {
          LOG.warn("Error while reading edits from disk. Will try again.", elie);
        } catch (InterruptedException ie) {
          // interrupter should have already set shouldRun to false
          continue;
        } catch (Throwable t) {
          LOG.fatal("Unknown error encountered while tailing edits. " +
              "Shutting down standby NN.", t);
          terminate(1, t);
        }
        //睡眠时间
        // 线程休眠sleepTimeMs时间后继续工作
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted", e);
        }
      }
    }
  }
  /**
   * Manage the 'active namenode proxy'. This cannot just be the a single proxy since we could
   * failover across a number of NameNodes, rather than just between an active and a standby.
   * <p>
   * We - lazily - get a proxy to one of the configured namenodes and attempt to make the request
   * against it. If it doesn't succeed, either because the proxy failed to be created or the request
   * failed, we try the next NN in the list. We try this up to the configuration maximum number of
   * retries before throwing up our hands. A working proxy is retained across attempts since we
   * expect the active NameNode to switch rarely.
   * <p>
   * This mechanism is <b>very bad</b> for cases where we care about being <i>fast</i>; it just
   * blindly goes and tries namenodes.
   */
  private abstract class MultipleNameNodeProxy<T> implements Callable<T> {

    /**
     * Do the actual work to the remote namenode via the {@link #cachedActiveProxy}.
     * @return the result of the work, if there is one
     * @throws IOException if the actions done to the proxy throw an exception.
     */
    protected abstract T doWork() throws IOException;

    public T call() throws IOException {
      // reset the loop count on success
      nnLoopCount = 0;
      while ((cachedActiveProxy = getActiveNodeProxy()) != null) {
        try {
          T ret = doWork();
          return ret;
        } catch (RemoteException e) {
          Throwable cause = e.unwrapRemoteException(StandbyException.class);
          // if its not a standby exception, then we need to re-throw it, something bad has happened
          if (cause == e) {
            throw e;
          } else {
            // it is a standby exception, so we try the other NN
            LOG.warn("Failed to reach remote node: " + currentNN
                + ", retrying with remaining remote NNs");
            cachedActiveProxy = null;
            // this NN isn't responding to requests, try the next one
            nnLoopCount++;
          }
        }
      }
      throw new IOException("Cannot find any valid remote NN to service request!");
    }

    private NamenodeProtocol getActiveNodeProxy() throws IOException {
      if (cachedActiveProxy == null) {
        while (true) {
          // if we have reached the max loop count, quit by returning null
          if ((nnLoopCount / nnCount) >= maxRetries) {
            return null;
          }

          currentNN = nnLookup.next();
          try {
            int rpcTimeout = conf.getInt(
                DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT);
            NamenodeProtocolPB proxy = RPC.waitForProxy(NamenodeProtocolPB.class,
                RPC.getProtocolVersion(NamenodeProtocolPB.class), currentNN.getIpcAddress(), conf,
                rpcTimeout, Long.MAX_VALUE);
            cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
            break;
          } catch (IOException e) {
            LOG.info("Failed to reach " + currentNN, e);
            // couldn't even reach this NN, try the next one
            nnLoopCount++;
          }
        }
      }
      assert cachedActiveProxy != null;
      return cachedActiveProxy;
    }
  }
}
