package dacp.repository

import java.nio.channels.FileChannel
import java.nio.file.{Files,Path,LinkOption,StandardCopyOption,StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.security.SecureRandom

/** Default publisher credential is private, persistent, and never exposed over HTTP. */
private[repository] object PublishToken {
  def path(root:Path):Path=root.resolve(".publish-token")
  def load(root:Path):String=Repository.synchronized {
    val channel=FileChannel.open(root.resolve(".publish.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)
    val lock=channel.lock()
    try {
      val target=path(root)
      if(!Files.exists(target,LinkOption.NOFOLLOW_LINKS)) {
        val random=new Array[Byte](32);new SecureRandom().nextBytes(random)
        val token=random.map(b=>f"${b & 255}%02x").mkString
        val file=Files.createTempFile(root,".token-",".tmp",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
        try {Files.writeString(file,token+"\n");Files.move(file,target,StandardCopyOption.ATOMIC_MOVE)}finally Files.deleteIfExists(file)
      }
      require(Files.isRegularFile(target,LinkOption.NOFOLLOW_LINKS) && Files.size(target)<=4097,"invalid publisher token file")
      Files.setPosixFilePermissions(target,PosixFilePermissions.fromString("rw-------"))
      Files.readString(target).stripSuffix("\n")
    }finally {lock.release();channel.close()}
  }
}
