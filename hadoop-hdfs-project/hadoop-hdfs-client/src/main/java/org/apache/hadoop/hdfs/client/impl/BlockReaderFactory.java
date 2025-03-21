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
package org.apache.hadoop.hdfs.client.impl;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.USE_RECEIPT_VERIFICATION;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.ReplicaAccessor;
import org.apache.hadoop.hdfs.ReplicaAccessorBuilder;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.ShortCircuitReplicaCreator;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplicaInfo;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderFactory implements ShortCircuitReplicaCreator {
  static final Logger LOG = LoggerFactory.getLogger(BlockReaderFactory.class);

  public static class FailureInjector {
    public void injectRequestFileDescriptorsFailure() throws IOException {
      // do nothing
    }
    public boolean getSupportsReceiptVerification() {
      return true;
    }
  }

  @VisibleForTesting
  static ShortCircuitReplicaCreator
      createShortCircuitReplicaInfoCallback = null;

  private final DfsClientConf conf;

  /**
   * Injects failures into specific operations during unit tests.
   */
  private static FailureInjector failureInjector = new FailureInjector();

  /**
   * The file name, for logging and debugging purposes.
   */
  private String fileName;

  /**
   * The block ID and block pool ID to use.
   */
  private ExtendedBlock block;

  /**
   * The block token to use for security purposes.
   */
  private Token<BlockTokenIdentifier> token;

  /**
   * The offset within the block to start reading at.
   */
  private long startOffset;

  /**
   * If false, we won't try to verify the block checksum.
   */
  private boolean verifyChecksum;

  /**
   * The name of this client.
   */
  private String clientName;

  /**
   * The DataNode we're talking to.
   */
  private DatanodeInfo datanode;

  /**
   * StorageType of replica on DataNode.
   */
  private StorageType storageType;

  /**
   * If false, we won't try short-circuit local reads.
   */
  private boolean allowShortCircuitLocalReads;

  /**
   * The ClientContext to use for things like the PeerCache.
   */
  private ClientContext clientContext;

  /**
   * Number of bytes to read. Must be set to a non-negative value.
   */
  private long length = -1;

  /**
   * Caching strategy to use when reading the block.
   */
  private CachingStrategy cachingStrategy;

  /**
   * Socket address to use to connect to peer.
   */
  private InetSocketAddress inetSocketAddress;

  /**
   * Remote peer factory to use to create a peer, if needed.
   */
  private RemotePeerFactory remotePeerFactory;

  /**
   * UserGroupInformation to use for legacy block reader local objects,
   * if needed.
   */
  private UserGroupInformation userGroupInformation;

  /**
   * Configuration to use for legacy block reader local objects, if needed.
   */
  private Configuration configuration;

  /**
   * Information about the domain socket path we should use to connect to the
   * local peer-- or null if we haven't examined the local domain socket.
   */
  private DomainSocketFactory.PathInfo pathInfo;

  /**
   * The remaining number of times that we'll try to pull a socket out of the
   * cache.
   */
  private int remainingCacheTries;

  public BlockReaderFactory(DfsClientConf conf) {
    this.conf = conf;
    this.remainingCacheTries = conf.getNumCachedConnRetry();
  }

  public BlockReaderFactory setFileName(String fileName) {
    this.fileName = fileName;
    return this;
  }

  public BlockReaderFactory setBlock(ExtendedBlock block) {
    this.block = block;
    return this;
  }

  public BlockReaderFactory setBlockToken(Token<BlockTokenIdentifier> token) {
    this.token = token;
    return this;
  }

  public BlockReaderFactory setStartOffset(long startOffset) {
    this.startOffset = startOffset;
    return this;
  }

  public BlockReaderFactory setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
    return this;
  }

  public BlockReaderFactory setClientName(String clientName) {
    this.clientName = clientName;
    return this;
  }

  public BlockReaderFactory setDatanodeInfo(DatanodeInfo datanode) {
    this.datanode = datanode;
    return this;
  }

  public BlockReaderFactory setStorageType(StorageType storageType) {
    this.storageType = storageType;
    return this;
  }

  public BlockReaderFactory setAllowShortCircuitLocalReads(
      boolean allowShortCircuitLocalReads) {
    this.allowShortCircuitLocalReads = allowShortCircuitLocalReads;
    return this;
  }

  public BlockReaderFactory setClientCacheContext(
      ClientContext clientContext) {
    this.clientContext = clientContext;
    return this;
  }

  public BlockReaderFactory setLength(long length) {
    this.length = length;
    return this;
  }

  public BlockReaderFactory setCachingStrategy(
      CachingStrategy cachingStrategy) {
    this.cachingStrategy = cachingStrategy;
    return this;
  }

  public BlockReaderFactory setInetSocketAddress (
      InetSocketAddress inetSocketAddress) {
    this.inetSocketAddress = inetSocketAddress;
    return this;
  }

  public BlockReaderFactory setUserGroupInformation(
      UserGroupInformation userGroupInformation) {
    this.userGroupInformation = userGroupInformation;
    return this;
  }

  public BlockReaderFactory setRemotePeerFactory(
      RemotePeerFactory remotePeerFactory) {
    this.remotePeerFactory = remotePeerFactory;
    return this;
  }

  public BlockReaderFactory setConfiguration(
      Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  @VisibleForTesting
  public static void setFailureInjectorForTesting(FailureInjector injector) {
    failureInjector = injector;
  }

  /**
   * Build a BlockReader with the given options.
   *
   * This function will do the best it can to create a block reader that meets
   * all of our requirements.  We prefer short-circuit block readers
   * (BlockReaderLocal and BlockReaderLocalLegacy) over remote ones, since the
   * former avoid the overhead of socket communication.  If short-circuit is
   * unavailable, our next fallback is data transfer over UNIX domain sockets,
   * if dfs.client.domain.socket.data.traffic has been enabled.  If that doesn't
   * work, we will try to create a remote block reader that operates over TCP
   * sockets.
   *
   * There are a few caches that are important here.
   *
   * The ShortCircuitCache stores file descriptor objects which have been passed
   * from the DataNode.
   *
   * The DomainSocketFactory stores information about UNIX domain socket paths
   * that we not been able to use in the past, so that we don't waste time
   * retrying them over and over.  (Like all the caches, it does have a timeout,
   * though.)
   *
   * The PeerCache stores peers that we have used in the past.  If we can reuse
   * one of these peers, we avoid the overhead of re-opening a socket.  However,
   * if the socket has been timed out on the remote end, our attempt to reuse
   * the socket may end with an IOException.  For that reason, we limit our
   * attempts at socket reuse to dfs.client.cached.conn.retry times.  After
   * that, we create new sockets.  This avoids the problem where a thread tries
   * to talk to a peer that it hasn't talked to in a while, and has to clean out
   * every entry in a socket cache full of stale entries.
   *
   * @return The new BlockReader.  We will not return null.
   *
   * @throws InvalidToken
   *             If the block token was invalid.
   *         InvalidEncryptionKeyException
   *             If the encryption key was invalid.
   *         Other IOException
   *             If there was another problem.
   */
  public BlockReader build() throws IOException {
    Preconditions.checkNotNull(configuration);
    Preconditions
        .checkState(length >= 0, "Length must be set to a non-negative value");
    BlockReader reader = tryToCreateExternalBlockReader();
    if (reader != null) {
      return reader;
    }
    //1.首先构建短路读共享内存
    //todo 重点去看这个配置，中间加载了很多配置，比如socket
    final ShortCircuitConf scConf = conf.getShortCircuitConf();
    try {
      if (scConf.isShortCircuitLocalReads() && allowShortCircuitLocalReads) {
        if (clientContext.getUseLegacyBlockReaderLocal()) {
          reader = getLegacyBlockReaderLocal();
          if (reader != null) {
            LOG.trace("{}: returning new legacy block reader local.", this);
            return reader;
          }
        } else {
          //2.本地磁盘
          reader = getBlockReaderLocal();
          if (reader != null) {
            LOG.trace("{}: returning new block reader local.", this);
            return reader;
          }
        }
      }
      //3.tcp读取
      if (scConf.isDomainSocketDataTraffic()) {
        reader = getRemoteBlockReaderFromDomain();
        if (reader != null) {
          LOG.trace("{}: returning new remote block reader using UNIX domain "
              + "socket on {}", this, pathInfo.getPath());
          return reader;
        }
      }
    } catch (IOException e) {
      LOG.debug("Block read failed. Getting remote block reader using TCP", e);
    }
    Preconditions.checkState(!DFSInputStream.tcpReadsDisabledForTesting,
        "TCP reads were disabled for testing, but we failed to " +
        "do a non-TCP read.");
    //todo 重点看看tcp socket读
    return getRemoteBlockReaderFromTcp();
  }

  private BlockReader tryToCreateExternalBlockReader() {
    List<Class<? extends ReplicaAccessorBuilder>> clses =
        conf.getReplicaAccessorBuilderClasses();
    for (Class<? extends ReplicaAccessorBuilder> cls : clses) {
      try {
        ByteArrayDataOutput bado = ByteStreams.newDataOutput();
        token.write(bado);
        byte tokenBytes[] = bado.toByteArray();

        Constructor<? extends ReplicaAccessorBuilder> ctor =
            cls.getConstructor();
        ReplicaAccessorBuilder builder = ctor.newInstance();
        long visibleLength = startOffset + length;
        ReplicaAccessor accessor = builder.
            setAllowShortCircuitReads(allowShortCircuitLocalReads).
            setBlock(block.getBlockId(), block.getBlockPoolId()).
            setGenerationStamp(block.getGenerationStamp()).
            setBlockAccessToken(tokenBytes).
            setClientName(clientName).
            setConfiguration(configuration).
            setFileName(fileName).
            setVerifyChecksum(verifyChecksum).
            setVisibleLength(visibleLength).
            build();
        if (accessor == null) {
          LOG.trace("{}: No ReplicaAccessor created by {}",
              this, cls.getName());
        } else {
          return new ExternalBlockReader(accessor, visibleLength, startOffset);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to construct new object of type " +
            cls.getName(), t);
      }
    }
    return null;
  }


  /**
   * Get {@link BlockReaderLocalLegacy} for short circuited local reads.
   * This block reader implements the path-based style of local reads
   * first introduced in HDFS-2246.
   */
  private BlockReader getLegacyBlockReaderLocal() throws IOException {
    LOG.trace("{}: trying to construct BlockReaderLocalLegacy", this);
    if (!DFSUtilClient.isLocalAddress(inetSocketAddress)) {
      LOG.trace("{}: can't construct BlockReaderLocalLegacy because the address"
          + "{} is not local", this, inetSocketAddress);
      return null;
    }
    if (clientContext.getDisableLegacyBlockReaderLocal()) {
      PerformanceAdvisory.LOG.debug("{}: can't construct " +
          "BlockReaderLocalLegacy because " +
          "disableLegacyBlockReaderLocal is set.", this);
      return null;
    }
    IOException ioe;
    try {
      return BlockReaderLocalLegacy.newBlockReader(conf,
          userGroupInformation, configuration, fileName, block, token,
          datanode, startOffset, length, storageType);
    } catch (RemoteException remoteException) {
      ioe = remoteException.unwrapRemoteException(
                InvalidToken.class, AccessControlException.class);
    } catch (IOException e) {
      ioe = e;
    }
    if ((!(ioe instanceof AccessControlException)) &&
        isSecurityException(ioe)) {
      // Handle security exceptions.
      // We do not handle AccessControlException here, since
      // BlockReaderLocalLegacy#newBlockReader uses that exception to indicate
      // that the user is not in dfs.block.local-path-access.user, a condition
      // which requires us to disable legacy SCR.
      throw ioe;
    }
    LOG.warn(this + ": error creating legacy BlockReaderLocal.  " +
        "Disabling legacy local reads.", ioe);
    clientContext.setDisableLegacyBlockReaderLocal();
    return null;
  }

  private BlockReader getBlockReaderLocal() throws IOException {
    LOG.trace("{}: trying to construct a BlockReaderLocal for short-circuit "
        + " reads.", this);
    if (pathInfo == null) {
      pathInfo = clientContext.getDomainSocketFactory()
          .getPathInfo(inetSocketAddress, conf.getShortCircuitConf());
    }
    if (!pathInfo.getPathState().getUsableForShortCircuit()) {
      PerformanceAdvisory.LOG.debug("{}: {} is not usable for short circuit; " +
              "giving up on BlockReaderLocal.", this, pathInfo);
      return null;
    }
    ShortCircuitCache cache = clientContext.getShortCircuitCache();
    ExtendedBlockId key = new ExtendedBlockId(block.getBlockId(),
        block.getBlockPoolId());
    ShortCircuitReplicaInfo info = cache.fetchOrCreate(key, this);
    InvalidToken exc = info.getInvalidTokenException();
    if (exc != null) {
      LOG.trace("{}: got InvalidToken exception while trying to construct "
          + "BlockReaderLocal via {}", this, pathInfo.getPath());
      throw exc;
    }
    if (info.getReplica() == null) {
      PerformanceAdvisory.LOG.debug("{}: failed to get " +
          "ShortCircuitReplica. Cannot construct " +
          "BlockReaderLocal via {}", this, pathInfo.getPath());
      return null;
    }
    return new BlockReaderLocal.Builder(conf.getShortCircuitConf()).
        setFilename(fileName).
        setBlock(block).
        setStartOffset(startOffset).
        setShortCircuitReplica(info.getReplica()).
        setVerifyChecksum(verifyChecksum).
        setCachingStrategy(cachingStrategy).
        setStorageType(storageType).
        build();
  }

  /**
   * Fetch a pair of short-circuit block descriptors from a local DataNode.
   *
   * @return    Null if we could not communicate with the datanode,
   *            a new ShortCircuitReplicaInfo object otherwise.
   *            ShortCircuitReplicaInfo objects may contain either an
   *            InvalidToken exception, or a ShortCircuitReplica object ready to
   *            use.
   */
  @Override
  public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
    if (createShortCircuitReplicaInfoCallback != null) {
      ShortCircuitReplicaInfo info =
          createShortCircuitReplicaInfoCallback.createShortCircuitReplicaInfo();
      if (info != null) return info;
    }
    LOG.trace("{}: trying to create ShortCircuitReplicaInfo.", this);
    BlockReaderPeer curPeer;
    while (true) {
      curPeer = nextDomainPeer();
      if (curPeer == null) break;
      if (curPeer.fromCache) remainingCacheTries--;
      DomainPeer peer = (DomainPeer)curPeer.peer;
      Slot slot = null;
      ShortCircuitCache cache = clientContext.getShortCircuitCache();
      try {
        MutableBoolean usedPeer = new MutableBoolean(false);
        slot = cache.allocShmSlot(datanode, peer, usedPeer,
            new ExtendedBlockId(block.getBlockId(), block.getBlockPoolId()),
            clientName);
        if (usedPeer.booleanValue()) {
          LOG.trace("{}: allocShmSlot used up our previous socket {}.  "
              + "Allocating a new one...", this, peer.getDomainSocket());
          curPeer = nextDomainPeer();
          if (curPeer == null) break;
          peer = (DomainPeer)curPeer.peer;
        }
        ShortCircuitReplicaInfo info = requestFileDescriptors(peer, slot);
        clientContext.getPeerCache().put(datanode, peer);
        return info;
      } catch (IOException e) {
        if (slot != null) {
          cache.freeSlot(slot);
        }
        if (curPeer.fromCache) {
          // Handle an I/O error we got when using a cached socket.
          // These are considered less serious, because the socket may be stale.
          LOG.debug("{}: closing stale domain peer {}", this, peer, e);
          IOUtilsClient.cleanup(LOG, peer);
        } else {
          // Handle an I/O error we got when using a newly created socket.
          // We temporarily disable the domain socket path for a few minutes in
          // this case, to prevent wasting more time on it.
          LOG.warn(this + ": I/O error requesting file descriptors.  " +
              "Disabling domain socket " + peer.getDomainSocket(), e);
          IOUtilsClient.cleanup(LOG, peer);
          clientContext.getDomainSocketFactory()
              .disableDomainSocketPath(pathInfo.getPath());
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Request file descriptors from a DomainPeer.
   *
   * @param peer   The peer to use for communication.
   * @param slot   If non-null, the shared memory slot to associate with the
   *               new ShortCircuitReplica.
   *
   * @return  A ShortCircuitReplica object if we could communicate with the
   *          datanode; null, otherwise.
   * @throws  IOException If we encountered an I/O exception while communicating
   *          with the datanode.
   */
  private ShortCircuitReplicaInfo requestFileDescriptors(DomainPeer peer,
          Slot slot) throws IOException {
    ShortCircuitCache cache = clientContext.getShortCircuitCache();
    final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(peer.getOutputStream()));
    SlotId slotId = slot == null ? null : slot.getSlotId();
    new Sender(out).requestShortCircuitFds(block, token, slotId, 1,
        failureInjector.getSupportsReceiptVerification());
    DataInputStream in = new DataInputStream(peer.getInputStream());
    BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
        PBHelperClient.vintPrefixed(in));
    DomainSocket sock = peer.getDomainSocket();
    failureInjector.injectRequestFileDescriptorsFailure();
    switch (resp.getStatus()) {
    case SUCCESS:
      byte buf[] = new byte[1];
      FileInputStream[] fis = new FileInputStream[2];
      sock.recvFileInputStreams(fis, buf, 0, buf.length);
      ShortCircuitReplica replica = null;
      try {
        if (fis[0] == null || fis[1] == null) {
          throw new IOException("the datanode " + datanode + " failed to " +
              "pass a file descriptor (might have reached open file limit).");
        }

        ExtendedBlockId key =
            new ExtendedBlockId(block.getBlockId(), block.getBlockPoolId());
        if (buf[0] == USE_RECEIPT_VERIFICATION.getNumber()) {
          LOG.trace("Sending receipt verification byte for slot {}", slot);
          sock.getOutputStream().write(0);
        }
        replica = new ShortCircuitReplica(key, fis[0], fis[1], cache,
            Time.monotonicNow(), slot);
        return new ShortCircuitReplicaInfo(replica);
      } catch (IOException e) {
        // This indicates an error reading from disk, or a format error.  Since
        // it's not a socket communication problem, we return null rather than
        // throwing an exception.
        LOG.warn(this + ": error creating ShortCircuitReplica.", e);
        return null;
      } finally {
        if (replica == null) {
          IOUtilsClient.cleanup(DFSClient.LOG, fis[0], fis[1]);
        }
      }
    case ERROR_UNSUPPORTED:
      if (!resp.hasShortCircuitAccessVersion()) {
        LOG.warn("short-circuit read access is disabled for " +
            "DataNode " + datanode + ".  reason: " + resp.getMessage());
        clientContext.getDomainSocketFactory()
            .disableShortCircuitForPath(pathInfo.getPath());
      } else {
        LOG.warn("short-circuit read access for the file " +
            fileName + " is disabled for DataNode " + datanode +
            ".  reason: " + resp.getMessage());
      }
      return null;
    case ERROR_ACCESS_TOKEN:
      String msg = "access control error while " +
          "attempting to set up short-circuit access to " +
          fileName + resp.getMessage();
      LOG.debug("{}:{}", this, msg);
      return new ShortCircuitReplicaInfo(new InvalidToken(msg));
    default:
      final long expiration =
          clientContext.getDomainSocketFactory().getPathExpireSeconds();
      String disableMsg = "disabled temporarily for " + expiration + " seconds";
      if (expiration == 0) {
        disableMsg = "not disabled";
      }
      LOG.warn("{}: unknown response code {} while attempting to set up "
              + "short-circuit access. {}. Short-circuit read for "
              + "DataNode {} is {} based on {}.",
          this, resp.getStatus(), resp.getMessage(), datanode,
          disableMsg, DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY);
      clientContext.getDomainSocketFactory()
          .disableShortCircuitForPath(pathInfo.getPath());
      return null;
    }
  }

  /**
   * Get a BlockReaderRemote that communicates over a UNIX domain socket.
   *
   * @return The new BlockReader, or null if we failed to create the block
   * reader.
   *
   * @throws InvalidToken    If the block token was invalid.
   * Potentially other security-related execptions.
   */
  private BlockReader getRemoteBlockReaderFromDomain() throws IOException {
    if (pathInfo == null) {
      pathInfo = clientContext.getDomainSocketFactory()
          .getPathInfo(inetSocketAddress, conf.getShortCircuitConf());
    }
    if (!pathInfo.getPathState().getUsableForDataTransfer()) {
      PerformanceAdvisory.LOG.debug("{}: not trying to create a " +
          "remote block reader because the UNIX domain socket at {}" +
           " is not usable.", this, pathInfo);
      return null;
    }
    LOG.trace("{}: trying to create a remote block reader from the UNIX domain "
        + "socket at {}", this, pathInfo.getPath());

    while (true) {
      BlockReaderPeer curPeer = nextDomainPeer();
      if (curPeer == null) break;
      if (curPeer.fromCache) remainingCacheTries--;
      DomainPeer peer = (DomainPeer)curPeer.peer;
      BlockReader blockReader = null;
      try {
        blockReader = getRemoteBlockReader(peer);
        return blockReader;
      } catch (IOException ioe) {
        IOUtilsClient.cleanup(LOG, peer);
        if (isSecurityException(ioe)) {
          LOG.trace("{}: got security exception while constructing a remote "
                  + " block reader from the unix domain socket at {}",
              this, pathInfo.getPath(), ioe);
          throw ioe;
        }
        if (curPeer.fromCache) {
          // Handle an I/O error we got when using a cached peer.  These are
          // considered less serious because the underlying socket may be stale.
          LOG.debug("Closed potentially stale domain peer {}", peer, ioe);
        } else {
          // Handle an I/O error we got when using a newly created domain peer.
          // We temporarily disable the domain socket path for a few minutes in
          // this case, to prevent wasting more time on it.
          LOG.warn("I/O error constructing remote block reader.  Disabling " +
              "domain socket " + peer.getDomainSocket(), ioe);
          clientContext.getDomainSocketFactory()
              .disableDomainSocketPath(pathInfo.getPath());
          return null;
        }
      } finally {
        if (blockReader == null) {
          IOUtilsClient.cleanup(LOG, peer);
        }
      }
    }
    return null;
  }

  /**
   * Get a BlockReaderRemote that communicates over a TCP socket.
   *
   * @return The new BlockReader.  We will not return null, but instead throw
   *         an exception if this fails.
   *
   * @throws InvalidToken
   *             If the block token was invalid.
   *         InvalidEncryptionKeyException
   *             If the encryption key was invalid.
   *         Other IOException
   *             If there was another problem.
   */
  private BlockReader getRemoteBlockReaderFromTcp() throws IOException {
    LOG.trace("{}: trying to create a remote block reader from a TCP socket",
        this);
    BlockReader blockReader = null;
    while (true) {

      BlockReaderPeer curPeer = null;
      Peer peer = null;
      try {
        //todo 重要，改方法实例化BlockReaderPeer类，包括设置socket相关参数
        curPeer = nextTcpPeer();
        if (curPeer.fromCache) remainingCacheTries--;
        peer = curPeer.peer;
        //todo 进入当前方法， 开始读取数据
        blockReader = getRemoteBlockReader(peer);
        return blockReader;
      } catch (IOException ioe) {
        if (isSecurityException(ioe)) {
          LOG.trace("{}: got security exception while constructing a remote "
              + "block reader from {}", this, peer, ioe);
          throw ioe;
        }
        if ((curPeer != null) && curPeer.fromCache) {
          // Handle an I/O error we got when using a cached peer.  These are
          // considered less serious, because the underlying socket may be
          // stale.
          LOG.debug("Closed potentially stale remote peer {}", peer, ioe);
        } else {
          // Handle an I/O error we got when using a newly created peer.
          LOG.warn("I/O error constructing remote block reader.", ioe);
          throw ioe;
        }
      } finally {
        if (blockReader == null) {
          IOUtilsClient.cleanup(LOG, peer);
        }
      }
    }
  }

  public static class BlockReaderPeer {
    final Peer peer;
    final boolean fromCache;

    BlockReaderPeer(Peer peer, boolean fromCache) {
      this.peer = peer;
      this.fromCache = fromCache;
    }
  }

  /**
   * Get the next DomainPeer-- either from the cache or by creating it.
   *
   * @return the next DomainPeer, or null if we could not construct one.
   */
  private BlockReaderPeer nextDomainPeer() {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, true);
      if (peer != null) {
        LOG.trace("nextDomainPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    DomainSocket sock = clientContext.getDomainSocketFactory().
        createSocket(pathInfo, conf.getSocketTimeout());
    if (sock == null) return null;
    return new BlockReaderPeer(new DomainPeer(sock), false);
  }

  /**
   * Get the next TCP-based peer-- either from the cache or by creating it.
   *
   * @return the next Peer, or null if we could not construct one.
   *
   * @throws IOException  If there was an error while constructing the peer
   *                      (such as an InvalidEncryptionKeyException)
   */
  private BlockReaderPeer nextTcpPeer() throws IOException {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, false);
      if (peer != null) {
        LOG.trace("nextTcpPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    try {
      //todo 实例化Peer，进到org.apache.hadoop.hdfs.DFSClient.newConnectedPeer方法中查看实例化参数
      Peer peer = remotePeerFactory.newConnectedPeer(inetSocketAddress, token,
          datanode);
      LOG.trace("nextTcpPeer: created newConnectedPeer {}", peer);
      return new BlockReaderPeer(peer, false);
    } catch (IOException e) {
      LOG.trace("nextTcpPeer: failed to create newConnectedPeer connected to"
          + "{}", datanode);
      throw e;
    }
  }

  /**
   * Determine if an exception is security-related.
   *
   * We need to handle these exceptions differently than other IOExceptions.
   * They don't indicate a communication problem.  Instead, they mean that there
   * is some action the client needs to take, such as refetching block tokens,
   * renewing encryption keys, etc.
   *
   * @param ioe    The exception
   * @return       True only if the exception is security-related.
   */
  private static boolean isSecurityException(IOException ioe) {
    return (ioe instanceof InvalidToken) ||
            (ioe instanceof InvalidEncryptionKeyException) ||
            (ioe instanceof InvalidBlockTokenException) ||
            (ioe instanceof AccessControlException);
  }

  @SuppressWarnings("deprecation")
  private BlockReader getRemoteBlockReader(Peer peer) throws IOException {
    //todo 获得client跟dn距离
    int networkDistance = clientContext.getNetworkDistance(datanode);
    //todo 进入当前方法
    return BlockReaderRemote.newBlockReader(
        fileName, block, token, startOffset, length,
        verifyChecksum, clientName, peer, datanode,
        clientContext.getPeerCache(), cachingStrategy,
        networkDistance);
  }

  @Override
  public String toString() {
    return "BlockReaderFactory(fileName=" + fileName + ", block=" + block + ")";
  }

  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }
}
