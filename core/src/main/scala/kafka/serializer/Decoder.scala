/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.serializer

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import kafka.utils.VerifiableProperties
import org.apache.kafka.common.record.{NotExpandingByteBufferOutputStream, ByteBufferOutputStream}

/**
  * A decoder is a method of turning byte arrays into objects.
  * An implementation is required to provide a constructor that
  * takes a VerifiableProperties instance.
  */
trait Decoder[T] {
  def fromBytes(bytes: Array[Byte]): T
}

/**
  * The default implementation does nothing, just returns the same byte array it takes in.
  */
class DefaultDecoder(props: VerifiableProperties = null) extends Decoder[Array[Byte]] {
  def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
}

/**
  * The string decoder translates bytes into strings. It uses UTF8 by default but takes
  * an optional property serializer.encoding to control this.
  */
class StringDecoder(props: VerifiableProperties = null) extends Decoder[String] {
  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, encoding)
  }
}

class StringGzipDecoder(props: VerifiableProperties = null) extends Decoder[String] {
  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): String = {

    val b4: Int = (bytes(bytes.length - 4) & 0xFF)
    val b3: Int = (bytes(bytes.length - 3) & 0xFF)
    val b2: Int = (bytes(bytes.length - 2) & 0xFF)
    val b1: Int = (bytes(bytes.length - 1) & 0xFF)
    val fullSize: Int = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4

    val out = new NotExpandingByteBufferOutputStream(ByteBuffer.allocate(fullSize))
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))

    var readCount: Int = 0
    val buffer: Array[Byte] = Array.ofDim[Byte](1024)

    while ( {
      readCount = in.read(buffer);
      readCount != -1
    }) {
//      System.out.println("ReadCount: " + readCount);
      out.write(buffer, 0, readCount)
    }

//    out.buffer().flip
    //    val result : Array[Byte] = Array.ofDim[Byte](out.buffer().remaining())
    //    return new String({out.buffer().get(result,0,result.length); result}, encoding)
    return new String(out.buffer().array(), encoding)

  }
}
