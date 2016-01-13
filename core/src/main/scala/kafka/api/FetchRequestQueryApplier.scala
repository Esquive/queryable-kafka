package kafka.api

import java.io.{InputStreamReader, BufferedReader}
import java.util
import java.util.concurrent.atomic.AtomicLong

import kafka.common.TopicAndPartition
import kafka.message.{ByteBufferMessageSet, CompressionFactory, CompressionCodec, Message}
import org.apache.kafka.common.record.ByteBufferInputStream

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by ericfalk on 13/01/16.
  */
object FetchRequestQueryApplier {

  def applyQueriesToResponse(topicsAndQueries: Map[String,String],
                             fetchPartitionData: Map[TopicAndPartition,FetchResponsePartitionData],
                             responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit): Unit = {

      val newMessages = new ListBuffer[Message]
      val queriedResponse = new collection.mutable.HashMap[TopicAndPartition, FetchResponsePartitionData]

      //For each topic we check if we have a query for it.
      fetchPartitionData.foreach(data => {

        //We rely on the fact that the messages are ordered. Although Threading might be an issue we will see.
        var firstOffset = -1l
        var compressionCodec: CompressionCodec = null

        //  TODO: if (fetchMetadata.topicsAndQueries.contains(data._1.topic)) {
        if (topicsAndQueries.contains("pcmd")) {

          //We get the query
          val query = topicsAndQueries.get(data._1.topic)

          //For each message we apply the the query
          data._2.messages.foreach(messageAndOffset => {
            firstOffset = messageAndOffset.offset
            val message = messageAndOffset.message
            compressionCodec = message.compressionCodec
            //TODO: I think I will have to uncompress the full message (with the key and all flags)
            val reader = new BufferedReader(new InputStreamReader(CompressionFactory.apply(message.compressionCodec, new ByteBufferInputStream(message.payload))))

            //TODO:Write directly in the buffer
            val queriedMessage = new StringBuilder()
            var line: String = null
            var isFirstIt = true
            while ( {
              line = reader.readLine();
              line != null
            }) {
              if(!isFirstIt){
                queriedMessage.append(System.lineSeparator())
              }
              //TODO: Add a separator in the query config
              val columns = line.split("-")
              queriedMessage.append(columns(0)).append("-").append(columns(1))
            }
            val messageBytes = queriedMessage.toString.getBytes
            val newMessage = new Message(messageBytes, util.Arrays.copyOfRange(message.buffer.array(), Message.KeyOffset, message.keySize), message.compressionCodec, 0, messageBytes.length)

            newMessages += newMessage
          })

          queriedResponse.put(data._1, new FetchResponsePartitionData(data._2.error, data._2.hw,
            new ByteBufferMessageSet(compressionCodec, new AtomicLong(firstOffset), newMessages: _*)))
          newMessages.clear()

        } else {
          queriedResponse.put(data._1, data._2)
        }

      })
     responseCallback(queriedResponse.toMap)
  }

}
