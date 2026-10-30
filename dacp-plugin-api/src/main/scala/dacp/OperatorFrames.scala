package dacp


import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream, EOFException}

/** Language-neutral operator stdout / Flight envelope. Logs belong on stderr. */
case class OperatorFrame(metadata: Array[Byte], data: Array[Byte] = Array.emptyByteArray)
object OperatorFrames {
  val maxHeader = 65536
  val maxData = 8 * 1024 * 1024
  def write(out: OutputStream, frame: OperatorFrame): Unit = {
    require(frame.metadata.nonEmpty && frame.metadata.length <= maxHeader && frame.data.length <= maxData, "operator frame exceeds limits")
    val d = new DataOutputStream(out)
    d.writeInt(frame.metadata.length); d.writeInt(frame.data.length); d.write(frame.metadata); d.write(frame.data); d.flush()
  }
  def read(in: InputStream): Option[OperatorFrame] = {
    val first = in.read()
    if(first < 0) return None
    val d = new DataInputStream(in)
    val header = (first << 24) | (d.readUnsignedByte() << 16) | (d.readUnsignedByte() << 8) | d.readUnsignedByte()
    val size = d.readInt()
    require(header > 0 && header <= maxHeader && size >= 0 && size <= maxData, "invalid operator frame length")
    val metadata = new Array[Byte](header); val payload = new Array[Byte](size)
    d.readFully(metadata); d.readFully(payload)
    Some(OperatorFrame(metadata,payload))
  }
  def encode(frame: OperatorFrame): Array[Byte] = { val out = new java.io.ByteArrayOutputStream(); write(out,frame); out.toByteArray }
  def decode(bytes: Array[Byte]): OperatorFrame = {
    val in = new java.io.ByteArrayInputStream(bytes)
    val frame = read(in).getOrElse(throw new IllegalArgumentException("empty frame"))
    require(in.available() == 0,"trailing frame data"); frame
  }
}
