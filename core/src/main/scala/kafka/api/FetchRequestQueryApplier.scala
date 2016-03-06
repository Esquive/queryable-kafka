package kafka.api

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel}
import java.util.Scanner
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import kafka.common.TopicAndPartition
import kafka.message._
import org.apache.commons.io.input.BoundedInputStream

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by ericfalk on 13/01/16.
  */
object FetchRequestQueryApplier {

  /**
    *
    * @param topicsAndQueries
    * @param fetchPartitionData
    * @param responseCallback
    */
  def applyQueriesToResponse(topicsAndQueries: Map[String, String],
                             fetchPartitionData: Map[TopicAndPartition, FetchResponsePartitionData],
                             responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit): Unit = {

    val newMessages = new ListBuffer[Message]
    val queriedResponse = new collection.mutable.HashMap[TopicAndPartition, FetchResponsePartitionData]

    //For each topic we check if we have a query for it.
    fetchPartitionData.foreach(data => {

      //We rely on the fact that the messages are ordered. Although Threading might be an issue we will see.
      var firstOffset = -1l

      if (topicsAndQueries.contains(data._1.topic)) {

        //We get the query
        val query = parseQuery(topicsAndQueries.get(data._1.topic).getOrElse("select *"))

        //Get the metadata about messages in the buffer
        var start = data._2.messages.getStart
        val end = data._2.messages.getEnd
        val channel = data._2.messages.getChannel
        if(start != -1) {
          while (start < end) {

            //parse the buffer retrieve the message Meta
            val msgMeta = getNextMessageOffsetKeyValue(start, end, channel)

            if (msgMeta != null) {

              val key = msgMeta.get(Keys.msgKey).get.asInstanceOf[Array[Byte]]
              val valueSize = msgMeta.get(Keys.valueSize).get.toString.toInt
              val valueOffset = msgMeta.get(Keys.valueOffset).get.toString.toInt
              firstOffset = msgMeta.get(Keys.offset).get.toString.toLong
              start = msgMeta.get(Keys.nextLocation).get.toString.toInt

              //Set the channel position to start stream from that point
              channel.position(valueOffset)

              val valueReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                new BoundedInputStream(Channels.newInputStream(channel), valueSize)
              )))

              newMessages += handleMessage(query, valueReader, key, firstOffset)

              //Close the reader
//              valueReader.close()
            }
          }
          queriedResponse.put(data._1, new FetchResponsePartitionData(data._2.error, data._2.hw,
            new ByteBufferMessageSet(CompressionCodec.getCompressionCodec(0), new AtomicLong(firstOffset), newMessages: _*)))
          newMessages.clear()
        } else {
          queriedResponse.put(data._1, data._2)
        }
      } else {
        queriedResponse.put(data._1, data._2)
      }
    })

    responseCallback(queriedResponse.toMap)
  }

  /**
    *
    * @param query
    * @param reader
    * @param key
    * @param offset
    * @return
    */
  private def handleMessage(query: QueryPlan, reader: Reader, key: Array[Byte], offset: Long): Message = {

    var line: String = null
    var hadPreviousLine = false
    val result = new ByteArrayOutputStream(1024*1024)
    val out = new GZIPOutputStream(result)
    val queriedMessage = new StringBuilder()
    while ( {
      line = reader.asInstanceOf[BufferedReader].readLine();
      line != null
    }) {
      if (hadPreviousLine) {
        out.write(System.lineSeparator().getBytes())
      }

      //TODO: Add separator in config
      val result = applyQuery(line, query)
      if(result.length >0)
      {
        out.write(result)
        hadPreviousLine = true
      }
      else
        hadPreviousLine = false

    }
    out.flush()
    out.close()

    new Message(result.toByteArray, key, CompressionCodec.getCompressionCodec(0))
  }


  /**
    *
    * @param line
    * @param queryPlan
    * @return
    */
  private def applyQuery(line: String, queryPlan: QueryPlan): Array[Byte] = {

    var keep = true
    val columns = line.split(";")
    val result = new StringBuilder()

    //columns(queryPlan.conditionColumn)

    //Check the criteria

    if (queryPlan.conditionOperation != null) {
      queryPlan.conditionOperation match {
        case "=" => {
          if (!(columns(queryPlan.conditionColumn).toInt == queryPlan.conditionValue)) {
            keep = false
          }
        }
      }
    }

    if (keep) {
      //          for(i <- 0 until queryPlan.columns.length){
      //             result.append(columns(i))
      //             if( !(i == queryPlan.columns.length) ) { result.append(";")}
      //          }

      for (i <- queryPlan.columns) {
        result.append(columns(i)).append(";")
      }

    }

    return result.toString.getBytes()
  }

  /**
    * A very simple Query parser
    *
    * @param query
    * @return
    */
  private def parseQuery(query: String): QueryPlan = {

    //Strip away all white spaces
    var cQuery = query.replaceAll(" ", "")

    //Remove the Select Keyword
    cQuery = cQuery.toLowerCase
    cQuery = cQuery.replace("select", "")

    //TODO: Handle the case without where specified
    //split at the where keyword
    val columnsAndConditions = cQuery.split("where")

    //Get the columns
    var scanner = new Scanner(new ByteArrayInputStream(columnsAndConditions(0).getBytes)).useDelimiter(",")
    val columns = new ListBuffer[Int]
    while (scanner.hasNextInt()) {
      columns += scanner.nextInt()
    }

    //Get the condition and
    //TODO: Improve
    scanner = new Scanner(columnsAndConditions(1)).useDelimiter("=")
    val conditionColumn = if (scanner.hasNextInt()) scanner.nextInt() else -1
    //val conditionOperator = scanner.findInLine(Pattern.compile("(>=|<=|=|<|>"))
    val conditionValue = if (scanner.hasNextInt()) scanner.nextInt() else -1

    QueryPlan(columns.toList.sorted, conditionColumn, conditionValue, "=")
  }


  /**
    *
    * @param start
    * @param end
    * @param channel
    * @return
    */
  private def getNextMessageOffsetKeyValue(start: Int, end: Int, channel: FileChannel): Map[String, Any] = {
    //Set the location
    var location = start
    val sizeOffsetBuffer = ByteBuffer.allocate(12)

    //If the location is bigger as the end return
    if (location >= end)
      return null

    // read the size of the item
    sizeOffsetBuffer.rewind()
    channel.read(sizeOffsetBuffer, location)
    if (sizeOffsetBuffer.hasRemaining)
      return null

    sizeOffsetBuffer.rewind()
    val offset = sizeOffsetBuffer.getLong()
    val size = sizeOffsetBuffer.getInt()
    if (size < Message.MinHeaderSize)
      return null
    //    if(size > maxMessageSize)
    //      throw new CorruptRecordException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize))

    //Read the KeySize
    val sizeBuffer = ByteBuffer.allocate(Message.KeySizeLength)
    channel.read(sizeBuffer, location + 12 + Message.KeySizeOffset)
    if (sizeBuffer.hasRemaining)
      return null
    sizeBuffer.rewind()
    val keySize = sizeBuffer.getInt()

    //Read the key
    val keyBuffer = ByteBuffer.allocate(keySize)
    channel.read(keyBuffer, location + 12 + Message.KeyOffset)
    if (keyBuffer.hasRemaining)
      return null
    keyBuffer.rewind()
    val keyBytes = Array.ofDim[Byte](keySize)
    keyBuffer.get(keyBytes)

    //Read the value size
    val valueSizeOffset = location + 12 + Message.KeyOffset + keySize
    sizeBuffer.rewind()
    channel.read(sizeBuffer, valueSizeOffset)
    if (sizeBuffer.hasRemaining)
      return null
    sizeBuffer.rewind()
    val valueSize = sizeBuffer.getInt()

    //Read the uncompressed file
    sizeBuffer.rewind()
    channel.read(sizeBuffer, 12 + size - 4)
    if (sizeBuffer.hasRemaining)
      return null
    sizeBuffer.rewind()
    val fullSize = sizeBuffer.getInt()

    //TODO: How about reading the ful buffer here?

    // increment the location and return the item
    location += size + 12
    Map("KeySize" -> keySize, "ValueSize" -> valueSize, "nextLocation" -> location,
      "key" -> keyBytes, Keys.valueOffset -> (valueSizeOffset + Message.ValueSizeLength),
      Keys.fullSzie -> fullSize, Keys.offset -> offset)
  }




  private case object Keys {
    val keySize = "KeySize"
    val valueSize = "ValueSize"
    val nextLocation = "nextLocation"
    val msgKey = "key"
    val valueOffset = "valueOffset"
    val fullSzie = "fullSize"
    val offset = "offset"
  }

  private case class QueryPlan(columns: List[Int], conditionColumn: Int, conditionValue: Int, conditionOperation: String)

}







