/*
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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer
import javax.annotation.Nullable

import io.fury.Fury
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{SERIALIZER_EXTRA_DEBUG_INFO, SERIALIZER_OBJECT_STREAM_RESET}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}



class FurySerializer(conf: SparkConf) extends Serializer with Externalizable {
  private var counterReset = conf.get(SERIALIZER_OBJECT_STREAM_RESET)
  private var extraDebugInfo = conf.get(SERIALIZER_EXTRA_DEBUG_INFO)

  def newFury(classLoader: ClassLoader): Fury = {
    val fury = Fury
      .builder()
      .withScalaOptimizationEnabled(true)
      .withRefTracking(true)
      .withAsyncCompilation(true)
      .withClassLoader(classLoader)
      .build()
    fury
  }

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new FurySerializerInstance(this, counterReset, extraDebugInfo, classLoader)
  }
  override def writeExternal(out: ObjectOutput): Unit =
    Utils.tryOrIOException {
      out.writeInt(counterReset)
      out.writeBoolean(extraDebugInfo)
    }

  override def readExternal(in: ObjectInput): Unit =
    Utils.tryOrIOException {
      counterReset = in.readInt()
      extraDebugInfo = in.readBoolean()
    }
}

private[spark] class FurySerializerInstance(
    fs: FurySerializer,
    counterReset: Int,
    extraDebugInfo: Boolean,
    defaultClassLoader: ClassLoader)
    extends SerializerInstance {

  @Nullable private[this] var cachedFury: Fury = borrowFury(defaultClassLoader)

  private[serializer] def borrowFury(): Fury = {
    if (cachedFury != null) {
      val fury = cachedFury
      cachedFury = null
      fury
    } else {
      fs.newFury(defaultClassLoader)
    }
  }

  private[serializer] def borrowFury(loader: ClassLoader): Fury = {
    if (cachedFury != null && cachedFury.getClassLoader == loader) {
      val fury = cachedFury
      cachedFury = null
      fury
    } else {
      fs.newFury(defaultClassLoader)
    }
  }

  private[serializer] def releaseFury(fury: Fury): Unit = {
    if (cachedFury == null) {
      cachedFury = fury
    }
  }

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new FurySerializationStream(this, s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new FuryDeserializationStream(this, s, defaultClassLoader)
  }
  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new FuryDeserializationStream(this, s, defaultClassLoader)
  }

}

private[spark] class FurySerializationStream(
    serInstance: FurySerializerInstance,
    outStream: OutputStream,
    counterReset: Int,
    extraDebugInfo: Boolean)
    extends SerializationStream {
  private[this] var output = new ObjectOutputStream(outStream)
  private var counter = 0
  @transient private[this] var fury: Fury = serInstance.borrowFury()

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      fury.serializeJavaObjectAndClass(output, t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      output.reset()
      counter = 0
    }
    this
  }

  override def flush(): Unit = {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }
  override def close(): Unit = {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseFury(fury)
        fury = null
        output = null
      }
    }
  }
}

private[spark] class FuryDeserializationStream(
    serInstance: FurySerializerInstance,
    inStream: InputStream,
    loader: ClassLoader)
    extends DeserializationStream {

  private[this] var input: ObjectInputStream = new ObjectInputStream(inStream) {

    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }

    override def resolveProxyClass(ifaces: Array[String]): Class[_] = {
      // scalastyle:off classforname
      val resolved = ifaces.map(iface => Class.forName(iface, false, loader))
      // scalastyle:on classforname
      java.lang.reflect.Proxy.getProxyClass(loader, resolved: _*)
    }

  }

  @transient private[this] var fury: Fury = serInstance.borrowFury(loader)

  override def readObject[T: ClassTag](): T = fury.deserialize(input).asInstanceOf[T]

  override def close(): Unit = {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseFury(fury)
        fury = null
        input = null
      }
    }
  }
}
