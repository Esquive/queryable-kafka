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

import java.util.concurrent.TimeUnit

import kafka.api.FetchResponsePartitionData
import kafka.api.PartitionFetchInfo
import kafka.common.TopicAndPartition
import kafka.message.MessageAndOffset
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionFetchInfo) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchOnlyLeader: Boolean,
                         fetchOnlyCommitted: Boolean,
                         isFromFollower: Boolean,
                         fetchPartitionStatus: Map[TopicAndPartition, FetchPartitionStatus]) {

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
                          "onlyLeader:" + fetchOnlyLeader + ", "
                          "onlyCommitted: " + fetchOnlyCommitted + ", "
                          "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: This broker does not know of some partitions it tries to fetch
   * Case C: The fetch offset locates not on the last segment of the log
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   *
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicAndPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val replica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
            val endOffset =
              if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark
              else
                replica.logEndOffset

            if (endOffset.offsetOnOlderSegment(fetchOffset)) {
              // Case C, this can happen when the new fetch operation is on a truncated leader
              debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicAndPartition))
              return forceComplete()
            } else if (fetchOffset.offsetOnOlderSegment(endOffset)) {
              // Case C, this can happen when the fetch operation is falling behind the current segment
              // or the partition has just rolled a new segment
              debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
              return forceComplete()
            } else if (fetchOffset.precedes(endOffset)) {
              // we need take the partition fetch size as upper bound when accumulating the bytes
              accumulatedSize += math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.fetchSize)
            }
          }
        } catch {
          case utpe: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
          case nle: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicAndPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // Case D
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    val logReadResults = replicaManager.readFromLocalLog(fetchMetadata.fetchOnlyLeader,
      fetchMetadata.fetchOnlyCommitted,
      fetchMetadata.fetchPartitionStatus.mapValues(status => status.fetchInfo))

    val fetchPartitionData = logReadResults.mapValues(result =>
      FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))

    val dataIt = fetchPartitionData.get(new TopicAndPartition("pcmd", 0)).iterator
    var value:FetchResponsePartitionData = null
    var message:MessageAndOffset = null
    if(dataIt.hasNext){
      value = dataIt.next()
      val messageIt = value.messages.iterator
      if(messageIt.hasNext){
        message = messageIt.next()
        val clear = new String(message.message.buffer.array())
        val pos = clear.charAt(0)
      }
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

