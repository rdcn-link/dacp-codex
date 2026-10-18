package dacp

import java.nio.file.{Files,Path}

/** External archive reader: the registry also supplies its format handler automatically. */
class TestContainerReader extends ContainerReader {
  val id = "test-container"
  val extensions = Set("testpack")
  def index(path: Path, publicName: String, demand: () => Unit): Seq[ContainerEntry] = {
    demand(); Seq(ContainerEntry("nested/entry.csv",false,None,None))
  }
  def withFile[A](path: Path, publicName: String, name: String, demand: () => Unit)(f: Path => A): A = {
    require(name == "nested/entry.csv","unknown test entry")
    val dir = Files.createTempDirectory("test-plugin-entry-")
    try {
      demand(); val child = dir.resolve("entry.csv")
      Files.write(child,Files.readAllBytes(path)); f(child)
    } finally Data.deleteTree(dir)
  }
}
