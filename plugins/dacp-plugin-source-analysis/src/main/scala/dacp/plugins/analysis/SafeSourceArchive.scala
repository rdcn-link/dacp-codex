package dacp.plugins.analysis


import dacp.operator.SourceArchive
import java.nio.file.{Files,Path}
import org.apache.commons.compress.archivers.{ArchiveEntry,ArchiveInputStream}
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.compress.archivers.tar.{TarArchiveInputStream,TarArchiveEntry}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

final class SafeSourceArchive extends SourceArchive {
  def supports(filename:String):Boolean=Seq(".zip",".tar",".tar.gz",".tgz").exists(filename.toLowerCase(java.util.Locale.ROOT).endsWith)
  def unpack(archive:Path,filename:String,destination:Path):Unit={
    require(Files.size(archive)<=64L*1024*1024,"source archive exceeds 64 MiB")
    val input=Files.newInputStream(archive)
    var stream:ArchiveInputStream[_ <: ArchiveEntry]=null
    try {
      val name=filename.toLowerCase(java.util.Locale.ROOT)
      stream=if(name.endsWith(".zip"))new ZipArchiveInputStream(input) else if(name.endsWith(".tar"))new TarArchiveInputStream(input) else new TarArchiveInputStream(new GzipCompressorInputStream(input))
      var entry=stream.getNextEntry;var files=0;var total=0L
      val seen=scala.collection.mutable.Set.empty[String]
      while(entry!=null){
        files+=1;require(files<=1000,"source archive exceeds 1000 entries")
        val name=entry.getName
        require(name.length<=1024 && !name.startsWith("/") && !name.contains("\\") && !name.split("/").contains("..") && !name.exists(Character.isISOControl),"unsafe source path")
        require(!name.split("/").contains(".") && seen.add(name),"duplicate or noncanonical source path")
        require(!Seq(".jar",".class",".pyc",".whl",".so",".dll",".exe",".zip",".tar",".tgz",".tar.gz").exists(name.toLowerCase(java.util.Locale.ROOT).endsWith),"compiled artifacts and nested archives are not accepted as source")
        entry match {case z:org.apache.commons.compress.archivers.zip.ZipArchiveEntry=>require(!z.isUnixSymlink,"source links are forbidden");case _=>()}
        val path=destination.resolve(name).normalize();require(path.startsWith(destination) && path!=destination,"unsafe source path")
        entry match {case t:TarArchiveEntry=>require(t.isDirectory || t.isFile,"source links and special files are forbidden");case _=>()}
        require(stream.canReadEntryData(entry),"unreadable source entry")
        if(entry.isDirectory)Files.createDirectories(path)
        else {
          Files.createDirectories(path.getParent)
          val out=Files.newOutputStream(path,java.nio.file.StandardOpenOption.CREATE_NEW)
          try {val buffer=new Array[Byte](8192);var size=0L;var n=stream.read(buffer);while(n>=0){size+=n;total+=n;require(size<=1024*1024 && total<=16L*1024*1024,"expanded source limit exceeded");out.write(buffer,0,n);n=stream.read(buffer)}}finally out.close()
        }
        entry=stream.getNextEntry
      }
    }finally {if(stream!=null)stream.close()else input.close()}
  }
}
