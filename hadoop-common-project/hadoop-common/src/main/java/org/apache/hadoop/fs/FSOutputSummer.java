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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;
import org.apache.log4j.helpers.LogLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends OutputStream {
  public static final Logger LOG = LoggerFactory.getLogger(FSOutputSummer.class);
  // data checksum
  private final DataChecksum sum;
  // internal buffer for storing data before it is checksumed
  private byte buf[];
  // internal buffer for storing checksum
  private byte checksum[];
  // The number of valid bytes in the buffer.
  private int count;
  
  // We want this value to be a multiple of 3 because the native code checksums
  // 3 chunks simultaneously. The chosen value of 9 strikes a balance between
  // limiting the number of JNI calls and flushing to the underlying stream
  // relatively frequently.
  private static final int BUFFER_NUM_CHUNKS = 9;
  
  protected FSOutputSummer(DataChecksum sum) {
    this.sum = sum;
    this.buf = new byte[sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS];
    this.checksum = new byte[getChecksumSize() * BUFFER_NUM_CHUNKS];
    this.count = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len > 0</code>, and its checksum
   */
  //org.apache.hadoop.hdfs.DFSOutputStream.writeChunk(byte[], int, int, byte[], int, int)
  protected abstract void writeChunk(byte[] b, int bOffset, int bLen,
      byte[] checksum, int checksumOffset, int checksumLen) throws IOException;
  
  /**
   * Check if the implementing OutputStream is closed and should no longer
   * accept writes. Implementations should do nothing if this stream is not
   * closed, and should throw an {@link IOException} if it is closed.
   * 
   * @throws IOException if this stream is already closed.
   */
  protected abstract void checkClosed() throws IOException;

  /** Write one byte */
  @Override
  public synchronized void write(int b) throws IOException {
    buf[count++] = (byte)b;
    if(count == buf.length) {
      //进入此方法
      flushBuffer();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids unnecessary data copy.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    
    checkClosed();
    //判断方法参数是否合法
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
   // 循环调用writer1（）写数据，每次写一个校验块
    for (int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   */
  private int write1(byte b[], int off, int len) throws IOException {
    if(count==0 && len>=buf.length) {
      // local buffer is empty and user buffer size >= local buffer size, so
      // simply checksum the user buffer and send it directly to the underlying
      // stream
      //如果buffer为null，并且当前写入数据大于一个chunk，则调用writeChecksumChunks()将数据与校验和写入IO中
      final int length = buf.length;
      writeChecksumChunks(b, off, length);
      return length;
    }
    
    // copy user data to local buffer
    //否则将数据写入buffer中
    int bytesToCopy = buf.length-count;
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
   //如果buffer满了，则调用flushBuffer()方法将缓存中数据写入到IO中
    LOG.debug("FSOutputSummer 类org.apache.hadoop.fs.FSOutputSummer.flushBuffer() 将buffler写满了，才进行写到io {}");
    if (count == buf.length) {
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream. 
   */
  protected synchronized void flushBuffer() throws IOException {
    flushBuffer(false, true);
  }

  /* Forces buffered output bytes to be checksummed and written out to
   * the underlying output stream. If there is a trailing partial chunk in the
   * buffer,
   * 1) flushPartial tells us whether to flush that chunk
   * 2) if flushPartial is true, keep tells us whether to keep that chunk in the
   * buffer (if flushPartial is false, it is always kept in the buffer)
   *
   * Returns the number of bytes that were flushed but are still left in the
   * buffer (can only be non-zero if keep is true).
   */
  protected synchronized int flushBuffer(boolean keep,
      boolean flushPartial) throws IOException {
    int bufLen = count;
    int partialLen = bufLen % sum.getBytesPerChecksum();
    int lenToFlush = flushPartial ? bufLen : bufLen - partialLen;
    if (lenToFlush != 0) {
      //todo 进入writeChecksumChunks方法
      //hdfs 文件 bolck文件块（128M）-> packet(64k) =127chunk -> chunk 512_chunksum
      writeChecksumChunks(buf, 0, lenToFlush);
      if (!flushPartial || keep) {
        count = partialLen;
        System.arraycopy(buf, bufLen - count, buf, 0, count);
      } else {
        count = 0;
      }
    }

    // total bytes left minus unflushed bytes left
    return count - (bufLen - lenToFlush);
  }

  /**
   * Checksums all complete data chunks and flushes them to the underlying
   * stream. If there is a trailing partial chunk, it is not flushed and is
   * maintained in the buffer.
   */
  public void flush() throws IOException {
    flushBuffer(false, false);
  }

  /**
   * Return the number of valid bytes currently in the buffer.
   */
  protected synchronized int getBufferedDataSize() {
    return count;
  }

  /** @return the size for a checksum. */
  protected int getChecksumSize() {
    return sum.getChecksumSize();
  }

  protected DataChecksum getDataChecksum() {
    return sum;
  }

  protected TraceScope createWriteTraceScope() {
    return null;
  }

  /** Generate checksums for the given data chunks and output chunks & checksums
   * to the underlying output stream.
   */
  private void writeChecksumChunks(byte b[], int off, int len)
  throws IOException {
    //计算chunk校验和 4字节
    sum.calculateChunkedSums(b, off, len, checksum, 0);
    TraceScope scope = createWriteTraceScope();
    try {
      //todo 按照chunk的大小遍历数据
      for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
        int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
        int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();
        //写chunk，进入DFSOutputStream 中的writeChunk方法（一个一个chunk的写入）
        writeChunk(b, off + i, chunkLen, checksum, ckOffset,
            getChecksumSize());
      }
    } finally {
      if (scope != null) {
        scope.close();
      }
    }
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
    return int2byte((int)sum.getValue(), new byte[checksumSize]);
  }

  static byte[] int2byte(int integer, byte[] bytes) {
    if (bytes.length != 0) {
      bytes[0] = (byte) ((integer >>> 24) & 0xFF);
      bytes[1] = (byte) ((integer >>> 16) & 0xFF);
      bytes[2] = (byte) ((integer >>> 8) & 0xFF);
      bytes[3] = (byte) ((integer >>> 0) & 0xFF);
      return bytes;
    }
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void setChecksumBufSize(int size) {
    this.buf = new byte[size];
    this.checksum = new byte[sum.getChecksumSize(size)];
    this.count = 0;
  }

  protected synchronized void resetChecksumBufSize() {
    setChecksumBufSize(sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS);
  }
}
