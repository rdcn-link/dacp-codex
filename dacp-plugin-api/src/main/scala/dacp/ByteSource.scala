package dacp

import java.io.{InputStream,FilterInputStream}
import java.nio.file.{Files,Path}
import java.nio.channels.Channels

/** Reopenable object content. name is a public filename, never a backend URI or credential.
  * Every returned stream is caller-owned. Implementations must check cancellation during reads.
  * All opens/ranges in one operation must refer to the same object version.
  */
trait ByteSource {
  def name:String
  def size:Option[Long]
  def version:Option[String]=None
  def open(check:()=>Unit):InputStream
  def supportsRanges:Boolean=false
  def readRange(offset:Long,length:Long,check:()=>Unit):InputStream=throw new UnsupportedOperationException("range reads unavailable")
  def localPath:Option[Path]=None
}
object ByteSource {
  def withCheck(source:ByteSource,check:()=>Unit):ByteSource=new ByteSource {
    def name:String=source.name
    def size:Option[Long]={check();source.size}
    override def version:Option[String]={check();source.version}
    override def supportsRanges:Boolean=source.supportsRanges
    override def localPath:Option[Path]={check();source.localPath}
    def open(next:()=>Unit):InputStream={def active():Unit={check();next()};active();checked(source.open(()=>active()),()=>active())}
    override def readRange(offset:Long,length:Long,next:()=>Unit):InputStream={def active():Unit={check();next()};active();checked(source.readRange(offset,length,()=>active()),()=>active())}
  }

  def local(path:Path):ByteSource=new ByteSource {
    def name:String=path.getFileName.toString
    def size:Option[Long]=Some(Files.size(path))
    override def version:Option[String]=Some(Files.getLastModifiedTime(path).toMillis+":"+Files.size(path))
    override def localPath:Option[Path]=Some(path)
    def open(check:()=>Unit):InputStream={check();checked(Files.newInputStream(path),check)}
    override val supportsRanges=true
    override def readRange(offset:Long,length:Long,check:()=>Unit):InputStream={
      require(offset>=0 && length>=0 && offset<=Files.size(path) && length<=Files.size(path)-offset,"invalid byte range")
      check();val channel=Files.newByteChannel(path)
      try {
        channel.position(offset)
        checked(new FilterInputStream(Channels.newInputStream(channel)) {
          private var remaining=length
          override def skip(n:Long):Long={val skipped=in.skip(math.max(0L,math.min(n,remaining)));remaining-=skipped;skipped}
          override def available():Int=math.min(in.available().toLong,remaining).toInt
          override def read():Int=if(remaining==0)-1 else {val n=super.read();if(n>=0)remaining-=1;n}
          override def read(b:Array[Byte],o:Int,n:Int):Int=if(n==0)0 else if(remaining==0)-1 else {val k=in.read(b,o,math.min(n.toLong,remaining).toInt);if(k>0)remaining-=k;k}
        },check)
      }catch{case t:Throwable=>channel.close();throw t}
    }
  }
  def checked(input:InputStream,check:()=>Unit):InputStream=new FilterInputStream(input) {
    override def read():Int={check();in.read()}
    override def read(b:Array[Byte],o:Int,n:Int):Int={check();in.read(b,o,n)}
    override def skip(n:Long):Long={check();in.skip(n)}
  }
  /** Explicit temporary staging for libraries which require a local file. The lease owns cleanup. */
  def materialize(source:ByteSource,check:()=>Unit):LocalBytes=source.localPath match {
    case Some(path)=>check();new LocalBytes(path,false)
    case None=>
      val path=Files.createTempFile("dacp-format-",".tmp")
      try {
        val in=source.open(check)
        try {
          val out=Files.newOutputStream(path)
          try {val b=new Array[Byte](65536);var n=0;do{check();n=in.read(b);if(n>0)out.write(b,0,n)}while(n>=0)}finally out.close()
        }finally in.close()
        new LocalBytes(path,true)
      }catch{case t:Throwable=>Files.deleteIfExists(path);throw t}
  }
}
final class LocalBytes private[dacp](val path:Path,temporary:Boolean) extends AutoCloseable {
  def close():Unit=if(temporary)Files.deleteIfExists(path)
}
