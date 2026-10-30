package dacp.plugins.analysis


import dacp.operator.SourceAnalyzer
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.{Files,Path}
import java.nio.charset.{StandardCharsets,CodingErrorAction}
import java.nio.ByteBuffer
import java.io.{InputStream,BufferedInputStream}
import org.apache.commons.compress.archivers.{ArchiveInputStream,ArchiveEntry}
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import scala.util.control.NonFatal

/** Conservative lexical triage, not semantic or dependency analysis, with bounded archive reads. */
final class BoundedSourceAnalyzer extends SourceAnalyzer {
  def id:String="bounded-source-rules-v1"
  def supports(filename:String):Boolean=Seq(".zip",".tar",".tar.gz",".tgz").exists(filename.toLowerCase(java.util.Locale.ROOT).endsWith)
  private case class Rule(id:String,severity:String,pattern:scala.util.matching.Regex,message:String)
  private val rules=Vector(
    Rule("external-network","high", "(?i)(https?://|java\\.net|javax\\.net|urllib|requests\\.|httpx\\.|socket[.(]|aiohttp|curl\\s|wget\\s|smtplib|ftplib|boto3)".r,"Network capability may disclose input data"),
    Rule("process-execution","high","(?i)(ProcessBuilder|Runtime\\s*\\.\\s*getRuntime|subprocess|os\\s*\\.\\s*(system|popen|exec)|scala\\.sys\\.process|/bin/(ba)?sh)".r,"Process execution requires manual review"),
    Rule("destructive-filesystem","critical","(?i)(shutil\\s*\\.\\s*rmtree|os\\s*\\.\\s*(remove|unlink)|Files\\s*\\.\\s*delete|rm\\s+-[a-z]*r|FileOutputStream\\s*\\(\\s*\"/|/etc/(passwd|shadow)|/var/run/docker.sock)".r,"Deletion, privileged paths or host access"),
    Rule("dynamic-code","high","(?i)(\\b(eval|exec)\\s*\\(|Class\\s*\\.\\s*forName|URLClassLoader|defineClass|ctypes|System\\s*\\.\\s*load|__import__|importlib|base64\\s*\\.\\s*b64decode)".r,"Dynamic code or native loading can hide behavior"),
    Rule("environment-secrets","medium","(?i)(System\\s*\\.\\s*getenv|os\\s*\\.\\s*environ|\\.ssh/|AWS_SECRET|PRIVATE.KEY|credential)".r,"Environment or credential access requires review"),
    Rule("file-write","medium","(?i)(Files\\s*\\.\\s*(write|newOutputStream)|FileOutputStream|open\\([^\\n]*,[\\s]*[\"'][wax]|pathlib[^\\n]*write_|chmod|chown)".r,"Filesystem writes may cause side effects")
  )
  def analyze(archive:Path,filename:String):com.fasterxml.jackson.databind.JsonNode={
    val mapper=new ObjectMapper();val report=mapper.createObjectNode().put("analyzer",id).put("started_at_ms",System.currentTimeMillis())
    val findings=report.putArray("findings");val gaps=report.putArray("coverage_gaps")
    var files=0;var sourceFiles=0;var bytes=0L;var gapCount=0;var highest=0;var stopped=false
    def gap(text:String):Unit={gapCount+=1;if(gaps.size()<50)gaps.add(text)}
    try {
      require(Files.size(archive)<=64L*1024*1024,"source archive exceeds scanner's 64 MiB compressed limit")
      val raw=new BufferedInputStream(Files.newInputStream(archive))
      var stream:ArchiveInputStream[_ <: ArchiveEntry]=null
      try {
        val name=filename.toLowerCase(java.util.Locale.ROOT)
        stream=if(name.endsWith(".zip"))new ZipArchiveInputStream(raw)
          else if(name.endsWith(".tar"))new TarArchiveInputStream(raw)
          else if(name.endsWith(".tar.gz") || name.endsWith(".tgz"))new TarArchiveInputStream(new GzipCompressorInputStream(raw))
          else throw new IllegalArgumentException("unsupported source archive")
        var entry=stream.getNextEntry
        while(entry!=null && !stopped){
          files+=1
          if(files>1000){gap("Archive exceeds 1000 entries");stopped=true}
          else if(!entry.isDirectory){
            val name=entry.getName
            require(name.length<=1024 && !name.startsWith("/") && !name.contains("\\") && !name.split("/").contains(".."),"unsafe source archive path")
            val link=entry match {case t:org.apache.commons.compress.archivers.tar.TarArchiveEntry=>t.isSymbolicLink || t.isLink;case _=>false}
            if(link || !stream.canReadEntryData(entry)){gap("Unreadable or linked archive entry: "+name.take(200));stopped=true}
            else {
              val out=new java.io.ByteArrayOutputStream();val buffer=new Array[Byte](8192);var count=stream.read(buffer)
              while(count>=0 && !stopped){
                bytes+=count
                if(bytes>16L*1024*1024 || out.size()+count>1024*1024){gap("Expanded size limit reached at "+name.take(200));stopped=true}
                else out.write(buffer,0,count)
                if(!stopped)count=stream.read(buffer)
              }
              if(!stopped){
                val extension=name.split('.').lastOption.getOrElse("").toLowerCase(java.util.Locale.ROOT)
                val supported=Set("py","java","scala","sh","bash","sbt","xml","json","yaml","yml","toml","properties","txt","md","cfg","ini")
                if(!supported(extension) && !Set("Dockerfile","Makefile")(name.split('/').last))gap("Unsupported file type: "+name.take(200))
                else {
                  val decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                  val text=decoder.decode(ByteBuffer.wrap(out.toByteArray)).toString
                  if(Set("py","java","scala","sh","bash")(extension))sourceFiles+=1
                  text.linesIterator.zipWithIndex.foreach{case(line,index)=>rules.foreach{rule=>if(rule.pattern.findFirstIn(line).isDefined){
                    highest=math.max(highest,if(rule.severity=="critical")5 else if(rule.severity=="high")3 else 1)
                    if(findings.size()<200)findings.addObject().put("rule",rule.id).put("severity",rule.severity).put("file",name.take(200)).put("line",index+1).put("evidence",line.take(300)).put("message",rule.message)
                  }}}
                }
              }
            }
          }
          if(!stopped)entry=stream.getNextEntry
        }
      }finally {if(stream!=null)stream.close()else raw.close()}
      if(sourceFiles==0)gap("No supported executable source files found")
      val score=if(sourceFiles==0)0 else math.min(5-highest,if(gapCount>0)2 else 5)
      report.put("status","complete").put("score",score).put("coverage_complete",gapCount==0)
    }catch{case NonFatal(e)=>report.put("status","failed").putNull("score");gap(Option(e.getMessage).getOrElse("analysis failed").take(300))}
    report.put("files",files).put("source_files",sourceFiles).put("expanded_bytes",bytes).put("coverage_gap_count",gapCount)
    report.put("finished_at_ms",System.currentTimeMillis()).put("notice","Static lexical triage can miss threats and flag safe code. A score of 5 means no rule matched within the scan coverage; it is not proof of safety or source/binary equivalence.")
    report
  }
}
