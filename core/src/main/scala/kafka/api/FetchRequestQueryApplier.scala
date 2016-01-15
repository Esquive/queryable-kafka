package kafka.api

import java.io.{ByteArrayInputStream, InputStreamReader, BufferedReader}
import java.util
import java.util.Scanner
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import kafka.common.TopicAndPartition
import kafka.message._
import org.apache.kafka.common.record.ByteBufferInputStream

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by ericfalk on 13/01/16.
  */
object FetchRequestQueryApplier {

  def applyQueriesToResponse(topicsAndQueries: Map[String, String],
                             fetchPartitionData: Map[TopicAndPartition, FetchResponsePartitionData],
                             responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit): Unit = {

    val newMessages = new ListBuffer[Message]
    val queriedResponse = new collection.mutable.HashMap[TopicAndPartition, FetchResponsePartitionData]

    //For each topic we check if we have a query for it.
    fetchPartitionData.foreach(data => {

      //We rely on the fact that the messages are ordered. Although Threading might be an issue we will see.
      var firstOffset = -1l
      var compressionCodec: CompressionCodec = null

      if (topicsAndQueries.contains(data._1.topic)) {
      //if (topicsAndQueries.contains("pcmd")) {

        //We get the query
        val query = parseQuery(topicsAndQueries.get(data._1.topic).getOrElse("select *"))
        var init = true

        //For each message we apply the the query: The iterator is used on purpose for automatic decryption of the messageset
        val wrapperMessages = data._2.messages.iterator
        while (wrapperMessages.hasNext) {
          val wrapperMessageAndOffset = wrapperMessages.next
          val wrapperMessage = wrapperMessageAndOffset.message
          if(init) {firstOffset = wrapperMessageAndOffset.offset; init=false}
          compressionCodec = wrapperMessage.compressionCodec



          compressionCodec match {
            case NoCompressionCodec => {
              newMessages += handleMessage(compressionCodec, wrapperMessageAndOffset,query)
            }
            case comp => {
              val wrapperMessageIt = ByteBufferMessageSet.deepIterator(wrapperMessage)
              while (wrapperMessageIt.hasNext) {
                newMessages += handleMessage(compressionCodec, wrapperMessageIt.next(),query)
              }

            }
          }
        }
        queriedResponse.put(data._1, new FetchResponsePartitionData(data._2.error, data._2.hw,
          new ByteBufferMessageSet(compressionCodec, new AtomicLong(firstOffset), newMessages: _*)))
        newMessages.clear()
      } else {
        queriedResponse.put(data._1, data._2)
      }
    })
    responseCallback(queriedResponse.toMap)
  }

  private def handleMessage(compressionCodec: CompressionCodec, messageAndOffset: MessageAndOffset, query: QueryPlan): Message = {
    val message = messageAndOffset.message
    val firstOffset = messageAndOffset.offset

    val reader = new BufferedReader(new InputStreamReader(new ByteBufferInputStream(message.payload)))
    val queriedMessage = new StringBuilder()
    var line: String = null
    var hadPreviousLine = false
    while ( {
      line = reader.readLine();
      line != null
    }) {
      if (hadPreviousLine) {
        queriedMessage.append(System.lineSeparator())
      }

      //TODO: Add a separator in the query config
    hadPreviousLine = applyQuery(line,query,queriedMessage)

    }

    //Remove the last new line should there be one.
//    if(queriedMessage.last.equals(System.lineSeparator()))(queriedMessage.deleteCharAt(queriedMessage.length - 1))
    if(queriedMessage.last.toString.equals(System.lineSeparator())){queriedMessage.deleteCharAt(queriedMessage.length - 1)}

    //TODO: See how I can get rid of more resources.
    //Close the reader
    reader.close()
    //Put a new message into that place.
    val messageBytes = queriedMessage.toString.getBytes
    new Message(messageBytes, util.Arrays.copyOfRange(message.buffer.array(), Message.KeyOffset, message.keySize), message.compressionCodec, 0, messageBytes.length)
  }

  private def applyQuery(line: String, queryPlan: QueryPlan, result: StringBuilder) : Boolean = {

    var keep = true
    val columns = line.split(";")


      columns(queryPlan.conditionColumn)

    //Check the criteria

    if(queryPlan.conditionOperation != null) {
      queryPlan.conditionOperation match {
        case "=" => {
          if (!(columns(queryPlan.conditionColumn).toInt == queryPlan.conditionValue)) {
            keep = false
          }
        }
      }
    }

    if(keep)
    {
          for(i <- 0 to queryPlan.columns.length){
             result.append(columns(i))
             if( !(i == queryPlan.columns.length)) { result.append(";")}
          }
    }

    return keep
  }

  /**
    * A very simple Query parser
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

  private case class QueryPlan(columns: List[Int], conditionColumn: Int, conditionValue: Int, conditionOperation: String)

}





