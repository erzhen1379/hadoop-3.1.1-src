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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Timer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;


/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;
  
  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  private final StopWatch quorumStopWatch;
  private final Timer timer;
  
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls, Timer timer) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>(timer);
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      });
    }
    return qr;
  }

  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    return create(calls, new Timer());
  }

  /**
   * Not intended for outside use.
   */
  private QuorumCall() {
    this(new Timer());
  }

  private QuorumCall(Timer timer) {
    // Only instantiated from factory method above
    this.timer = timer;
    this.quorumStopWatch = new StopWatch(timer);
  }

  /**
   * Used in conjunction with {@link #getQuorumTimeoutIncreaseMillis(long, int)}
   * to check for pauses.
   */
  private void restartQuorumStopWatch() {
    quorumStopWatch.reset().start();
  }

  /**
   * Check for a pause (e.g. GC) since the last time
   * {@link #restartQuorumStopWatch()} was called. If detected, return the
   * length of the pause; else, -1.
   * @param offset Offset the elapsed time by this amount; use if some amount
   *               of pause was expected
   * @param millis Total length of timeout in milliseconds
   * @return Length of pause, if detected, else -1
   */
  private long getQuorumTimeoutIncreaseMillis(long offset, int millis) {
    long elapsed = quorumStopWatch.now(TimeUnit.MILLISECONDS);
    long pauseTime = elapsed + offset;
    if (pauseTime > (millis * WAIT_PROGRESS_INFO_THRESHOLD)) {
      QuorumJournalManager.LOG.info("Pause detected while waiting for " +
          "QuorumCall response; increasing timeout threshold by pause time " +
          "of " + pauseTime + " ms.");
      return pauseTime;
    } else {
      return -1;
    }
  }

  
  /**
   * Wait for the quorum to achieve a certain number of responses.
   * 
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
      throws InterruptedException, TimeoutException {
    //minResponses 5
    //minSuccesses 3
    //maxExceptions 3
    //millis 20 秒  必须在20秒写入成功，不然就写入超时
    long st = timer.monotonicNow();
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);
    /**
     *计算超时时间 : 10:00:20
     */
    long et = st + millis;
    while (true) {  //多次重试
      //重置gc时间
      restartQuorumStopWatch();
      checkAssertionErrors();
      /**
       * 思路：
       * 写journode可以，无论写成功和失败，都要返回
       *
       * 写数据到jn，
       * 1，写成功
       * 2.写失败 （重试）
       */
      //第一种情况：写入成功+写入失败
      if (minResponses > 0 && countResponses() >= minResponses) return;
      //写入成功 >=3
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      //写入失败>3
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;
      //todo 获取当前时间

      /**
       * full gc -30 秒（导致当前线程停止运行）
       *
       * 第一次进来 ：获取当前时间 10:00:01
       *
       * 第二次进来: 获取当前时间 10:00:01+5+1 秒= 10：00：07
       *
       * fugc 10：00：31
       */
      long now = timer.monotonicNow();
      /**
       * 第一次进来：判断是否已经到了打印日志时间：10：00：01   -> 10：00：06  还没到打印日志时间
       * 第二次进来：判断是否已经到了打印日志时间：10：00：07   -> 10：00：06  到了打印日志时间
       */
      if (now > nextLogTime) {
        //计算已经等待多久，10：00：07  -10:00:00=7s
        long waited = now - st;
        //打印日志
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        //如果等待时间超过了，7 》 20+0.7就打印warn日志
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          //如果没有查过就打印info日志
          QuorumJournalManager.LOG.info(msg);
        }
        //重新计算加一次打印日志的时间：10：00：07 +1 =10：00：08
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }
      //可以超时的时间
      /**
       * 第一次进来，计算还剩多少时间， 10：00：20 -  10：00：01  花费19秒
       *  第二次进来，计算还剩多少时间， 10：00：20 -  10：00：07  花费13秒
       *  第n次进来 计算还剩余时间，10：00：20 - 10：00：21  -1
       */
      long rem = et - now;
      /**
       * 第一次进来：19<0 不成，所以不退出
       * 第二次进来：13<0 不成，所以不退出
       *
       *
       * fullgc  -11
       * 第n次进来
       *  剩余超时时间
       */
      if (rem <= 0) {
        // Increase timeout if a full GC occurred after restarting stopWatch
        long timeoutIncrease = getQuorumTimeoutIncreaseMillis(0, millis);
        if (timeoutIncrease > 0) {
          et += timeoutIncrease;
        } else {
          //todo 抛出一个超时时间，上游捕获
          throw new TimeoutException();
        }
      }
      restartQuorumStopWatch();
      /**
       * 第一次进来： Math.min(10:00:06   -  10:00:01)  ==>5
       * 第二次进来：  Math.min(13,10:00:08   -  10:00:06)  ==>2
       */
      rem = Math.min(rem, nextLogTime - now);
      /**
       * 第一次进来： Math.max(5,1)  ==>5
       * 第二次进来：  Math.max(2,1)  ==>2
       */
      rem = Math.max(rem, 1);
      /**
       * 第一次进来： wait 5秒
       * 第二次进来： wait 1秒
       */
      wait(rem);
      // Increase timeout if a full GC occurred after restarting stopWatch
      //gc 处理
      long timeoutIncrease = getQuorumTimeoutIncreaseMillis(-rem, millis);
      if (timeoutIncrease > 0) {
        et += timeoutIncrease;
      }
    }
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   * 
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing through.
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }
  
  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }
  
  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }
  
  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }
  
  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}
