/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.nio.ByteBuffer
import java.lang.{Long => JLong, Short => JShort}

import kafka.admin.AdminUtils
import kafka.api._
import kafka.cluster.Partition
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.{GroupCoordinator, JoinGroupResult}
import kafka.log._
import kafka.message.MessageSet
import kafka.network._
import kafka.network.RequestChannel.{Session, Response}
import kafka.security.auth.{Authorizer, ClusterAction, Group, Create, Describe, Operation, Read, Resource, Topic, Write}
import kafka.utils.{Logging, SystemTime, ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.errors.{InvalidTopicException, NotLeaderForPartitionException, UnknownTopicOrPartitionException,
ClusterAuthorizationException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{ListOffsetRequest, ListOffsetResponse, GroupCoordinatorRequest, GroupCoordinatorResponse, ListGroupsResponse,
DescribeGroupsRequest, DescribeGroupsResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
LeaveGroupRequest, LeaveGroupResponse, ResponseHeader, ResponseSend, SyncGroupRequest, SyncGroupResponse, LeaderAndIsrRequest, LeaderAndIsrResponse,
StopReplicaRequest, StopReplicaResponse}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicPartition, Node}

import scala.collection._
import scala.collection.JavaConverters._

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val coordinator: GroupCoordinator,
                val controller: KafkaController,
                val zkUtils: ZkUtils,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer]) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  // Store all the quota managers for each type of request
  val quotaManagers: Map[Short, ClientQuotaManager] = instantiateQuotaManagers(config)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestObj, request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
        case ApiKeys.PRODUCE_LARGE => handleProducerLargeRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        if (request.requestObj != null) {
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }

    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body.asInstanceOf[LeaderAndIsrRequest]

    try {
      def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
        // for each new leader or follower, call coordinator to handle consumer group migration.
        // this callback is invoked under the replica state change lock to ensure proper order of
        // leadership changes
        updatedLeaders.foreach { partition =>
          if (partition.topic == GroupCoordinator.GroupMetadataTopicName)
            coordinator.handleGroupImmigration(partition.partitionId)
        }
        updatedFollowers.foreach { partition =>
          if (partition.topic == GroupCoordinator.GroupMetadataTopicName)
            coordinator.handleGroupEmigration(partition.partitionId)
        }
      }

      val responseHeader = new ResponseHeader(correlationId)
      val leaderAndIsrResponse=
        if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
          val result = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, metadataCache, onLeadershipChange)
          new LeaderAndIsrResponse(result.errorCode, result.responseMap.mapValues(new JShort(_)).asJava)
        } else {
          val result = leaderAndIsrRequest.partitionStates.asScala.keys.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
          new LeaderAndIsrResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
        }

      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body.asInstanceOf[StopReplicaRequest]

    val responseHeader = new ResponseHeader(request.header.correlationId)
    val response =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        val (result, error) = replicaManager.stopReplicas(stopReplicaRequest)
        new StopReplicaResponse(error, result.asInstanceOf[Map[TopicPartition, JShort]].asJava)
      } else {
        val result = stopReplicaRequest.partitions.asScala.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
        new StopReplicaResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
      }

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val updateMetadataRequest = request.requestObj.asInstanceOf[UpdateMetadataRequest]

    authorizeClusterAction(request)

    replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache)

    val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]

    authorizeClusterAction(request)

    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      Errors.NONE.code, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
  }


  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]

    // reject the request immediately if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetCommitRequest.groupId))) {
      val errors = offsetCommitRequest.requestInfo.mapValues(_ => Errors.GROUP_AUTHORIZATION_FAILED.code)
      val response = OffsetCommitResponse(errors, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, response)))
      return
    }

    // filter non-exist topics
    val invalidRequestsInfo = offsetCommitRequest.requestInfo.filter { case (topicAndPartition, offsetMetadata) =>
      !metadataCache.contains(topicAndPartition.topic)
    }
    val filteredRequestInfo = (offsetCommitRequest.requestInfo -- invalidRequestsInfo.keys)

    val (authorizedRequestInfo, unauthorizedRequestInfo) =  filteredRequestInfo.partition {
      case (topicAndPartition, offsetMetadata) =>
        authorize(request.session, Read, new Resource(Topic, topicAndPartition.topic))
    }

    // the callback for sending an offset commit response
    def sendResponseCallback(commitStatus: immutable.Map[TopicAndPartition, Short]) {
      val mergedCommitStatus = commitStatus ++ unauthorizedRequestInfo.mapValues(_ => Errors.TOPIC_AUTHORIZATION_FAILED.code)

      mergedCommitStatus.foreach { case (topicAndPartition, errorCode) =>
        if (errorCode != Errors.NONE.code) {
          debug("Offset commit request with correlation id %d from client %s on partition %s failed due to %s"
            .format(offsetCommitRequest.correlationId, offsetCommitRequest.clientId,
              topicAndPartition, Errors.forCode(errorCode).exception.getClass.getName))
        }
      }
      val combinedCommitStatus = mergedCommitStatus ++ invalidRequestsInfo.map(_._1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      val response = OffsetCommitResponse(combinedCommitStatus, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else if (offsetCommitRequest.versionId == 0) {
      // for version 0 always store offsets to ZK
      val responseInfo = authorizedRequestInfo.map {
        case (topicAndPartition, metaAndError) => {
          val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic)
          try {
            if (metadataCache.getTopicMetadata(Set(topicAndPartition.topic), request.securityProtocol).size <= 0) {
              (topicAndPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            } else if (metaAndError.metadata != null && metaAndError.metadata.length > config.offsetMetadataMaxSize) {
              (topicAndPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
            } else {
              zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir + "/" +
                topicAndPartition.partition, metaAndError.offset.toString)
              (topicAndPartition, Errors.NONE.code)
            }
          } catch {
            case e: Throwable => (topicAndPartition, Errors.forException(e).code)
          }
        }
      }

      sendResponseCallback(responseInfo)
    } else {
      // for version 1 and beyond store offsets in offset manager

      // compute the retention time based on the request version:
      // if it is v1 or not specified by user, we can use the default retention
      val offsetRetention =
        if (offsetCommitRequest.versionId <= 1 ||
          offsetCommitRequest.retentionMs == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_RETENTION_TIME) {
          coordinator.offsetConfig.offsetsRetentionMs
        } else {
          offsetCommitRequest.retentionMs
        }

      // commit timestamp is always set to now.
      // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
      // expire timestamp is computed differently for v1 and v2.
      //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
      //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
      //   - If v2 we use the default expiration timestamp
      val currentTimestamp = SystemTime.milliseconds
      val defaultExpireTimestamp = offsetRetention + currentTimestamp
      val offsetData = authorizedRequestInfo.mapValues(offsetAndMetadata =>
        offsetAndMetadata.copy(
          commitTimestamp = currentTimestamp,
          expireTimestamp = {
            if (offsetAndMetadata.commitTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP)
              defaultExpireTimestamp
            else
              offsetRetention + offsetAndMetadata.commitTimestamp
          }
        )
      )

      // call coordinator to handle commit offset
      coordinator.handleCommitOffsets(
        offsetCommitRequest.groupId,
        offsetCommitRequest.memberId,
        offsetCommitRequest.groupGenerationId,
        offsetData,
        sendResponseCallback)
    }
  }

  private def authorize(session: Session, operation: Operation, resource: Resource): Boolean =
    authorizer.map(_.authorize(session, operation, resource)).getOrElse(true)

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]
    val numBytesAppended = produceRequest.sizeInBytes

    val (authorizedRequestInfo, unauthorizedRequestInfo) =  produceRequest.data.partition  {
      case (topicAndPartition, _) => authorize(request.session, Write, new Resource(Topic, topicAndPartition.topic))
    }

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicAndPartition, ProducerResponseStatus]) {

      val mergedResponseStatus = responseStatus ++ unauthorizedRequestInfo.mapValues(_ => ProducerResponseStatus(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1))

      var errorInResponse = false

      mergedResponseStatus.foreach { case (topicAndPartition, status) =>
        if (status.error != Errors.NONE.code) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            produceRequest.correlationId,
            produceRequest.clientId,
            topicAndPartition,
            Errors.forCode(status.error).exception.getClass.getName))
        }
      }

      def produceResponseCallback(delayTimeMs: Int) {

        if (produceRequest.requiredAcks == 0) {
          // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
          // the request, since no response is expected by the producer, the server will close socket server so that
          // the producer client will know that some error has happened and will refresh its metadata
          if (errorInResponse) {
            val exceptionsSummary = mergedResponseStatus.map { case (topicAndPartition, status) =>
              topicAndPartition -> Errors.forCode(status.error).exception.getClass.getName
            }.mkString(", ")
            info(
              s"Closing connection due to error during produce request with correlation id ${produceRequest.correlationId} " +
                s"from client id ${produceRequest.clientId} with ack=0\n" +
                s"Topic and partition to exceptions: $exceptionsSummary"
            )
            requestChannel.closeConnection(request.processor, request)
          } else {
            requestChannel.noOperation(request.processor, request)
          }
        } else {
          val response = ProducerResponse(produceRequest.correlationId,
                                          mergedResponseStatus,
                                          produceRequest.versionId,
                                          delayTimeMs)
          requestChannel.sendResponse(new RequestChannel.Response(request,
                                                                  new RequestOrResponseSend(request.connectionId,
                                                                                            response)))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds

      quotaManagers(ApiKeys.PRODUCE.id).recordAndMaybeThrottle(produceRequest.clientId,
                                                                   numBytesAppended,
                                                                   produceResponseCallback)
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = produceRequest.clientId == AdminUtils.AdminClientId

      // call the replica manager to append messages to the replicas
      replicaManager.appendMessages(
        produceRequest.ackTimeoutMs.toLong,
        produceRequest.requiredAcks,
        internalTopicsAllowed,
        authorizedRequestInfo,
        sendResponseCallback)

      // if the request is put into the purgatory, it will have a held reference
      // and hence cannot be garbage collected; hence we clear its data here in
      // order to let GC re-claim its memory since it is already appended to log
      produceRequest.emptyData()
    }
  }


  def handleProducerLargeRequest(request: RequestChannel.Request): Unit ={
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]
  }


  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) =  fetchRequest.requestInfo.partition {
      case (topicAndPartition, _) => authorize(request.session, Read, new Resource(Topic, topicAndPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ => FetchResponsePartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, MessageSet.Empty))

    // the callback for sending a fetch response
    def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
      val mergedResponseStatus = responsePartitionData ++ unauthorizedResponseStatus

      mergedResponseStatus.foreach { case (topicAndPartition, data) =>
        if (data.error != Errors.NONE.code) {
          debug("Fetch request with correlation id %d from client %s on partition %s failed due to %s"
            .format(fetchRequest.correlationId, fetchRequest.clientId,
            topicAndPartition, Errors.forCode(data.error).exception.getClass.getName))
        }
        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesOutRate.mark(data.messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.messages.sizeInBytes)
      }

      def fetchResponseCallback(delayTimeMs: Int) {
        val response = FetchResponse(fetchRequest.correlationId, mergedResponseStatus, fetchRequest.versionId, delayTimeMs)
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, response)))
      }


      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds

      // Do not throttle replication traffic
      if (fetchRequest.isFromFollower) {
        fetchResponseCallback(0)
      } else {
        quotaManagers(ApiKeys.FETCH.id).recordAndMaybeThrottle(fetchRequest.clientId,
                                                                   FetchResponse.responseSize(responsePartitionData
                                                                                                      .groupBy(_._1.topic),
                                                                                              fetchRequest.versionId),
                                                                   fetchResponseCallback)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        authorizedRequestInfo,
        fetchRequest.topicsAndQueries,
        sendResponseCallback)
    }
  }

  /**
   * Handle an offset request
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body.asInstanceOf[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.offsetData.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, List[JLong]().asJava)
    )

    val responseMap = authorizedRequestInfo.map(elem => {
      val (topicPartition, partitionData) = elem
      try {
        // ensure leader exists
        val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
          replicaManager.getLeaderReplicaIfLocal(topicPartition.topic, topicPartition.partition)
        else
          replicaManager.getReplicaOrException(topicPartition.topic, topicPartition.partition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicPartition,
                                        partitionData.timestamp,
                                        partitionData.maxNumOffsets)
          if (offsetRequest.replicaId != ListOffsetRequest.CONSUMER_REPLICA_ID) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE.code, offsets.map(new JLong(_)).asJava))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition, utpe.getMessage))
          (topicPartition,  new ListOffsetResponse.PartitionData(Errors.forException(utpe).code, List[JLong]().asJava))
        case nle: NotLeaderForPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition,nle.getMessage))
          (topicPartition,  new ListOffsetResponse.PartitionData(Errors.forException(nle).code, List[JLong]().asJava))
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicPartition,  new ListOffsetResponse.PartitionData(Errors.forException(e).code, List[JLong]().asJava))
      }
    })

    val mergedResponseMap = responseMap ++ unauthorizedResponseStatus

    val responseHeader = new ResponseHeader(correlationId)
    val response = new ListOffsetResponse(mergedResponseMap.asJava)

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
  }

  def fetchOffsets(logManager: LogManager, topicPartition: TopicPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(TopicAndPartition(topicPartition.topic, topicPartition.partition)) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP || timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
          Seq(0L)
        else
          Nil
    }
  }

  private def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray
    var offsetTimeArray: Array[(Long, Long)] = null
    if (segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for(i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if (segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  private def getTopicMetadata(topics: Set[String], securityProtocol: SecurityProtocol): Seq[TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol)
    if (topics.size > 0 && topicResponses.size != topics.size) {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == GroupCoordinator.GroupMetadataTopicName || config.autoCreateTopicsEnable) {
          try {
            if (topic == GroupCoordinator.GroupMetadataTopicName) {
              val aliveBrokers = metadataCache.getAliveBrokers
              val offsetsTopicReplicationFactor =
                if (aliveBrokers.length > 0)
                  Math.min(config.offsetsTopicReplicationFactor.toInt, aliveBrokers.length)
                else
                  config.offsetsTopicReplicationFactor.toInt
              AdminUtils.createTopic(zkUtils, topic, config.offsetsTopicPartitions,
                                     offsetsTopicReplicationFactor,
                                     coordinator.offsetsTopicConfigs)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
            }
            else {
              AdminUtils.createTopic(zkUtils, topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                   .format(topic, config.numPartitions, config.defaultReplicationFactor))
            }
            new TopicMetadata(topic, Seq.empty[PartitionMetadata], Errors.LEADER_NOT_AVAILABLE.code)
          } catch {
            case e: TopicExistsException => // let it go, possibly another broker created this topic
              new TopicMetadata(topic, Seq.empty[PartitionMetadata], Errors.LEADER_NOT_AVAILABLE.code)
            case itex: InvalidTopicException =>
              new TopicMetadata(topic, Seq.empty[PartitionMetadata], Errors.INVALID_TOPIC_EXCEPTION.code)
          }
        } else {
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
      }
      topicResponses.appendAll(responsesForNonExistentTopics)
    }
    topicResponses
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]

    //if topics is empty -> fetch all topics metadata but filter out the topic response that are not authorized
    val topics = if (metadataRequest.topics.isEmpty) {
      val topicResponses = metadataCache.getTopicMetadata(metadataRequest.topics.toSet, request.securityProtocol)
      topicResponses.map(_.topic).filter(topic => authorize(request.session, Describe, new Resource(Topic, topic))).toSet
    } else {
      metadataRequest.topics.toSet
    }

    //when topics is empty this will be a duplicate authorization check but given this should just be a cache lookup, it should not matter.
    var (authorizedTopics, unauthorizedTopics) = topics.partition(topic => authorize(request.session, Describe, new Resource(Topic, topic)))

    if (!authorizedTopics.isEmpty) {
      val topicResponses = metadataCache.getTopicMetadata(authorizedTopics, request.securityProtocol)
      if (config.autoCreateTopicsEnable && topicResponses.size != authorizedTopics.size) {
        val nonExistentTopics: Set[String] = topics -- topicResponses.map(_.topic).toSet
        authorizer.foreach {
          az => if (!az.authorize(request.session, Create, Resource.ClusterResource)) {
            authorizedTopics --= nonExistentTopics
            unauthorizedTopics ++= nonExistentTopics
          }
        }
      }
    }

    val unauthorizedTopicMetaData = unauthorizedTopics.map(topic => new TopicMetadata(topic, Seq.empty[PartitionMetadata], Errors.TOPIC_AUTHORIZATION_FAILED.code))

    val topicMetadata = if (authorizedTopics.isEmpty) Seq.empty[TopicMetadata] else getTopicMetadata(authorizedTopics, request.securityProtocol)
    val brokers = metadataCache.getAliveBrokers
    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    val response = new TopicMetadataResponse(brokers.map(_.getBrokerEndPoint(request.securityProtocol)), topicMetadata  ++ unauthorizedTopicMetaData, metadataRequest.correlationId)
    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  /*
   * Handle an offset fetch request
   */

  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.requestObj.asInstanceOf[OffsetFetchRequest]

    // reject the request immediately if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetFetchRequest.groupId))) {
      val authorizationError = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.GROUP_AUTHORIZATION_FAILED.code)
      val response = OffsetFetchResponse(offsetFetchRequest.requestInfo.map{ _ -> authorizationError}.toMap)
      requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, response)))
      return
    }

    val (authorizedTopicPartitions, unauthorizedTopicPartitions) = offsetFetchRequest.requestInfo.partition { topicAndPartition =>
      authorize(request.session, Describe, new Resource(Topic, topicAndPartition.topic))
    }

    val authorizationError = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.TOPIC_AUTHORIZATION_FAILED.code)
    val unauthorizedStatus = unauthorizedTopicPartitions.map(topicAndPartition => (topicAndPartition, authorizationError)).toMap

    val response = if (offsetFetchRequest.versionId == 0) {
      // version 0 reads offsets from ZK
      val responseInfo = authorizedTopicPartitions.map( topicAndPartition => {
        val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicAndPartition.topic)
        try {
          if (metadataCache.getTopicMetadata(Set(topicAndPartition.topic), request.securityProtocol).size <= 0) {
            (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)
          } else {
            val payloadOpt = zkUtils.readDataMaybeNull(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1
            payloadOpt match {
              case Some(payload) => (topicAndPartition, OffsetMetadataAndError(payload.toLong))
              case None => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)
            }
          }
        } catch {
          case e: Throwable =>
            (topicAndPartition, OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata,
              Errors.forException(e).code))
        }
      })

      OffsetFetchResponse(collection.immutable.Map(responseInfo: _*) ++ unauthorizedStatus, offsetFetchRequest.correlationId)
    } else {
      // version 1 reads offsets from Kafka;
      val offsets = coordinator.handleFetchOffsets(offsetFetchRequest.groupId, authorizedTopicPartitions).toMap

      // Note that we do not need to filter the partitions in the
      // metadata cache as the topic partitions will be filtered
      // in coordinator's offset manager through the offset cache
      OffsetFetchResponse(offsets ++ unauthorizedStatus, offsetFetchRequest.correlationId)
    }

    trace("Sending offset fetch response %s for correlation id %d to client %s."
          .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))

    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  def handleGroupCoordinatorRequest(request: RequestChannel.Request) {
    val groupCoordinatorRequest = request.body.asInstanceOf[GroupCoordinatorRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    if (!authorize(request.session, Describe, new Resource(Group, groupCoordinatorRequest.groupId))) {
      val responseBody = new GroupCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED.code, Node.noNode)
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      val partition = coordinator.partitionFor(groupCoordinatorRequest.groupId)

      // get metadata (and create the topic if necessary)
      val offsetsTopicMetadata = getTopicMetadata(Set(GroupCoordinator.GroupMetadataTopicName), request.securityProtocol).head
      val coordinatorEndpoint = offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).flatMap {
        partitionMetadata => partitionMetadata.leader
      }

      val responseBody = coordinatorEndpoint match {
        case None =>
          new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, Node.noNode())
        case Some(endpoint) =>
          new GroupCoordinatorResponse(Errors.NONE.code, new Node(endpoint.id, endpoint.host, endpoint.port))
      }

      trace("Sending consumer metadata %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request) {
    import JavaConverters._

    val describeRequest = request.body.asInstanceOf[DescribeGroupsRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    val groups = describeRequest.groupIds().asScala.map {
      case groupId =>
        if (!authorize(request.session, Describe, new Resource(Group, groupId))) {
          groupId -> DescribeGroupsResponse.GroupMetadata.forError(Errors.GROUP_AUTHORIZATION_FAILED)
        } else {
          val (error, summary) = coordinator.handleDescribeGroup(groupId)
          val members = summary.members.map { member =>
            val metadata = ByteBuffer.wrap(member.metadata)
            val assignment = ByteBuffer.wrap(member.assignment)
            new DescribeGroupsResponse.GroupMember(member.memberId, member.clientId, member.clientHost, metadata, assignment)
          }
          groupId -> new DescribeGroupsResponse.GroupMetadata(error.code, summary.state, summary.protocolType,
            summary.protocol, members.asJava)
        }
    }.toMap

    val responseBody = new DescribeGroupsResponse(groups.asJava)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def handleListGroupsRequest(request: RequestChannel.Request) {
    import JavaConverters._

    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (!authorize(request.session, Describe, Resource.ClusterResource)) {
      ListGroupsResponse.fromError(Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else {
      val (error, groups) = coordinator.handleListGroups()
      val allGroups = groups.map{ group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }
      new ListGroupsResponse(error.code, allGroups.asJava)
    }
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val joinGroupRequest = request.body.asInstanceOf[JoinGroupRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult) {
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      val responseBody = new JoinGroupResponse(joinResult.errorCode, joinResult.generationId, joinResult.subProtocol,
        joinResult.memberId, joinResult.leaderId, members)

      trace("Sending join group response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) {
      val responseBody = new JoinGroupResponse(
        Errors.GROUP_AUTHORIZATION_FAILED.code,
        JoinGroupResponse.UNKNOWN_GENERATION_ID,
        JoinGroupResponse.UNKNOWN_PROTOCOL,
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId
        Map.empty[String, ByteBuffer])
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // let the coordinator to handle join-group
      val protocols = joinGroupRequest.groupProtocols().map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      coordinator.handleJoinGroup(
        joinGroupRequest.groupId,
        joinGroupRequest.memberId,
        request.header.clientId,
        request.session.clientAddress.toString,
        joinGroupRequest.sessionTimeout,
        joinGroupRequest.protocolType,
        protocols,
        sendResponseCallback)
    }
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val syncGroupRequest = request.body.asInstanceOf[SyncGroupRequest]

    def sendResponseCallback(memberState: Array[Byte], errorCode: Short) {
      val responseBody = new SyncGroupResponse(errorCode, ByteBuffer.wrap(memberState))
      val responseHeader = new ResponseHeader(request.header.correlationId)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) {
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED.code)
    } else {
      coordinator.handleSyncGroup(
        syncGroupRequest.groupId(),
        syncGroupRequest.generationId(),
        syncGroupRequest.memberId(),
        syncGroupRequest.groupAssignment().mapValues(Utils.toArray(_)),
        sendResponseCallback
      )
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body.asInstanceOf[HeartbeatRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a heartbeat response
    def sendResponseCallback(errorCode: Short) {
      val response = new HeartbeatResponse(errorCode)
      trace("Sending heartbeat response %s for correlation id %d to client %s."
        .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    if (!authorize(request.session, Read, new Resource(Group, heartbeatRequest.groupId))) {
      val heartbeatResponse = new HeartbeatResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, heartbeatResponse)))
    }
    else {
      // let the coordinator to handle heartbeat
      coordinator.handleHeartbeat(
        heartbeatRequest.groupId(),
        heartbeatRequest.memberId(),
        heartbeatRequest.groupGenerationId(),
        sendResponseCallback)
    }
  }

  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  private def instantiateQuotaManagers(cfg: KafkaConfig): Map[Short, ClientQuotaManager] = {
    val producerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val consumerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val quotaManagers = Map[Short, ClientQuotaManager](
      ApiKeys.PRODUCE.id ->
              new ClientQuotaManager(producerQuotaManagerCfg, metrics, ApiKeys.PRODUCE.name, new org.apache.kafka.common.utils.SystemTime),
      ApiKeys.FETCH.id ->
              new ClientQuotaManager(consumerQuotaManagerCfg, metrics, ApiKeys.FETCH.name, new org.apache.kafka.common.utils.SystemTime)
    )
    quotaManagers
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body.asInstanceOf[LeaveGroupRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a leave-group response
    def sendResponseCallback(errorCode: Short) {
      val response = new LeaveGroupResponse(errorCode)
      trace("Sending leave group response %s for correlation id %d to client %s."
                    .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    if (!authorize(request.session, Read, new Resource(Group, leaveGroupRequest.groupId))) {
      val leaveGroupResponse = new LeaveGroupResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, leaveGroupResponse)))
    } else {
      // let the coordinator to handle leave-group
      coordinator.handleLeaveGroup(
        leaveGroupRequest.groupId(),
        leaveGroupRequest.memberId(),
        sendResponseCallback)
    }
  }

  def close() {
    quotaManagers.foreach { case (apiKey, quotaManager) =>
      quotaManager.shutdown()
    }
    info("Shutdown complete.")
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, ClusterAction, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }
}
