package dacp

import _root_.xstream.data._

import java.nio.file.Path
import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.poi.poifs.filesystem.{FileMagic, POIFSFileSystem}
import org.apache.poi.openxml4j.opc.{OPCPackage, PackageAccess}
import org.apache.poi.xssf.eventusermodel.XSSFReader
import org.apache.poi.util.XMLHelper
import org.apache.poi.ss.util.CellReference
import org.apache.poi.hssf.eventusermodel.{HSSFEventFactory, HSSFListener, HSSFRequest}
import org.apache.poi.hssf.record._
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler

/** Event readers buffer 16 rows; POI shared-string dictionaries may consume additional memory. */
class ExcelFormat extends FormatHandler {
  val id = "excel"; val extensions = Set("xls", "xlsx")
  override val records = true
  override def validateOptions(options: FileOptions): Unit = {
    require(options.parameters.keySet.subsetOf(Set("sheet","header_row")), "unknown Excel reader parameter")
    require(options.parameters.get("sheet").forall(_.isInstanceOf[String]), "sheet must be a string")
    headerRow(options)
  }
  private def sheet(options: FileOptions): Option[String] = options.parameters.get("sheet").map(_.toString)
  private def headerRow(options: FileOptions): Int = {
    val raw = options.parameters.getOrElse("header_row",Int.box(1))
    require(raw.isInstanceOf[java.lang.Number] && raw.toString.matches("[0-9]+"), "header_row must be a positive integer")
    val value = raw.toString.toInt
    require(value > 0,"header_row must be positive"); value
  }
  override def rows(source: ByteSource, options: FileOptions): Records.Rows = {
    validateOptions(options)
    val local=ByteSource.materialize(source,()=>())
    try {
      val reader=new ExcelRows(local.path,options)
      new Records.Rows {val names=reader.names;val iterator=reader.iterator;def close():Unit=try reader.close()finally local.close()}
    }catch{case t:Throwable=>local.close();throw t}
  }

  private class ExcelRows(path: Path, options: FileOptions) extends Records.Rows {
    private val queue = new ArrayBlockingQueue[Either[Throwable,Option[Vector[Any]]]](16)
    @volatile private var stopped = false
    private def send(value: Either[Throwable,Option[Vector[Any]]]): Unit = {
      if (stopped || Thread.currentThread().isInterrupted) throw new InterruptedException()
      queue.put(value)
    }
    private def sendError(error: FormatFailure): Unit =
      try { if (!stopped) send(Left(error)) } catch { case _: InterruptedException => () }
    private val worker = new Thread(() => {
      var header = false; var width = 0
      def row(index: Int, cells: Map[Int,Any]): Unit = {
        if (stopped || Thread.currentThread().isInterrupted) throw new InterruptedException()
        if (index + 1 == headerRow(options)) {
          if (cells.isEmpty) throw new FormatFailure("Excel header row is empty.")
          width = cells.keys.max + 1
          val used = mutable.Set.empty[String]
          val names = (0 until width).map { col =>
            val base = Option(cells.getOrElse(col,null)).map(_.toString.trim).filter(_.nonEmpty).getOrElse(s"column_${col+1}")
            var name = base; var suffix = 2
            while (used(name)) { name = s"${base}_$suffix"; suffix += 1 }
            used += name; name
          }.toVector
          header = true; send(Right(Some(names)))
        } else if (index + 1 > headerRow(options) && cells.values.exists(_ != null)) {
          if (!header) throw new FormatFailure("Excel header row was not found.")
          if (cells.exists { case (col,value) => col >= width && value != null })
            throw new FormatFailure("Excel data extends beyond the header columns.")
          send(Right(Some((0 until width).map(col => cells.getOrElse(col,null)).toVector)))
        }
      }
      try {
        val magic = FileMagic.valueOf(path.toFile)
        if (magic == FileMagic.OLE2) readXls(path,options,row)
        else if (magic == FileMagic.OOXML) readXlsx(path,options,row)
        else throw new FormatFailure("Invalid Excel workbook.")
        if (!header) throw new FormatFailure("Excel header row was not found.")
        send(Right(None))
      } catch {
        case _: InterruptedException => ()
        case e: FormatFailure => sendError(e)
        case scala.util.control.NonFatal(_) => sendError(new FormatFailure("Unable to read Excel workbook. Check the file and sheet configuration."))
      }
    }, "dacp-excel-reader")
    worker.setDaemon(true); worker.start()
    private def nextMessage(): Option[Vector[Any]] = queue.take() match {
      case Right(value) => value
      case Left(error) => close(); throw error
    }
    val names: Seq[String] = try nextMessage().getOrElse(throw new FormatFailure("Excel header row was not found.")).map(_.toString)
      catch { case t: Throwable => close(); throw t }
    val iterator: Iterator[Seq[Any]] = new Iterator[Seq[Any]] {
      private var current: Option[Vector[Any]] = None
      private var loaded = false; private var ended = false
      def hasNext: Boolean = {
        if (!loaded && !ended) { current = nextMessage(); loaded = true; ended = current.isEmpty }
        !ended
      }
      def next(): Seq[Any] = { if (!hasNext) throw new NoSuchElementException; loaded = false; current.get }
    }
    def close(): Unit = { stopped = true; worker.interrupt(); if (Thread.currentThread() != worker) worker.join(1000) }
  }
  private def select(names: Seq[String], options: FileOptions): String = sheet(options) match {
    case Some(name) if names.contains(name) => name
    case Some(_) => throw new FormatFailure("Configured Excel sheet was not found.")
    case None if names.size == 1 => names.head
    case _ => throw new FormatFailure("Excel workbooks with multiple sheets require a configured sheet.")
  }
  private def number(value: String): Any = {
    val n = value.toDouble
    if (n.isWhole && n >= Long.MinValue.toDouble && n < Long.MaxValue.toDouble) n.toLong else n
  }
  private def readXlsx(path: Path, options: FileOptions, emit: (Int,Map[Int,Any]) => Unit): Unit = {
    val pkg = OPCPackage.open(path.toFile,PackageAccess.READ)
    try {
      val reader = new XSSFReader(pkg)
      val names = mutable.ArrayBuffer.empty[String]
      val workbook = reader.getWorkbookData
      try {
        val parser = XMLHelper.newXMLReader()
        parser.setContentHandler(new DefaultHandler {
          override def startElement(uri: String, local: String, q: String, a: Attributes): Unit =
            if (local == "sheet") names += a.getValue("name")
        })
        parser.parse(new org.xml.sax.InputSource(workbook))
      } finally workbook.close()
      val selected = select(names.toSeq,options)
      val strings = reader.getSharedStringsTable
      val sheets = reader.getSheetsData.asInstanceOf[XSSFReader.SheetIterator]
      var found = false
      while (sheets.hasNext && !found) {
        val in = sheets.next()
        try if (sheets.getSheetName == selected) {
          found = true
          val parser = XMLHelper.newXMLReader()
          parser.setContentHandler(new DefaultHandler {
            var row = -1; var column = 0; var cellType = ""; var collecting = false
            var text = new StringBuilder
            val cells = mutable.Map.empty[Int,Any]
            override def startElement(uri: String, local: String, q: String, a: Attributes): Unit = {
              if (Thread.currentThread().isInterrupted) throw new InterruptedException()
              local match {
                case "row" => row = Option(a.getValue("r")).map(_.toInt-1).getOrElse(row+1); cells.clear()
                case "c" => column = new CellReference(a.getValue("r")).getCol.toInt; cellType = Option(a.getValue("t")).getOrElse(""); text = new StringBuilder
                case "v" | "t" => collecting = true
                case _ => ()
              }
            }
            override def characters(ch: Array[Char], start: Int, length: Int): Unit = if (collecting) text.appendAll(ch,start,length)
            override def endElement(uri: String, local: String, q: String): Unit = local match {
              case "v" | "t" => collecting = false
              case "c" =>
                val raw = text.toString
                val value: Any = if (raw.isEmpty) null else cellType match {
                  case "s" => strings.getItemAt(raw.toInt).getString
                  case "b" => raw == "1"
                  case "inlineStr" | "str" | "d" => raw
                  case "e" => throw new FormatFailure("Excel contains a cell error.")
                  case _ => number(raw)
                }
                cells(column) = value
              case "row" => emit(row,cells.toMap)
              case _ => ()
            }
          })
          parser.parse(new org.xml.sax.InputSource(in))
        } finally in.close()
      }
    } finally pkg.close()
  }
  private def readXls(path: Path, options: FileOptions, emit: (Int,Map[Int,Any]) => Unit): Unit = {
    val fs = new POIFSFileSystem(path.toFile,true)
    try {
      val bounds = mutable.ArrayBuffer.empty[BoundSheetRecord]
      var strings: SSTRecord = null
      var sheet = -1; var selected = ""; var active = false
      var row = -1; var pendingString = -1
      val cells = mutable.Map.empty[Int,Any]
      def flush(): Unit = if (row >= 0) { emit(row,cells.toMap); cells.clear(); row = -1 }
      def set(r: Int, c: Int, value: Any): Unit = { if (row != r) { flush(); row = r }; cells(c) = value }
      val request = new HSSFRequest()
      request.addListenerForAllRecords(new HSSFListener {
        override def processRecord(record: org.apache.poi.hssf.record.Record): Unit = {
          if (Thread.currentThread().isInterrupted) throw new InterruptedException()
          record match {
            case b: BoundSheetRecord => bounds += b
            case s: SSTRecord => strings = s
            case b: BOFRecord if b.getType == BOFRecord.TYPE_WORKSHEET =>
              if (selected.isEmpty) selected = select(bounds.map(_.getSheetname).toSeq,options)
              sheet += 1
              val ordered = BoundSheetRecord.orderByBofPosition(bounds.asJava)
              active = sheet < ordered.length && ordered(sheet).getSheetname == selected
            case _: EOFRecord if active => flush(); active = false
            case _ if !active => ()
            case n: NumberRecord => set(n.getRow,n.getColumn,number(n.getValue.toString))
            case n: RKRecord => set(n.getRow,n.getColumn,number(n.getRKNumber.toString))
            case n: MulRKRecord => (0 until n.getNumColumns).foreach(i => set(n.getRow,n.getFirstColumn+i,number(n.getRKNumberAt(i).toString)))
            case s: LabelSSTRecord => set(s.getRow,s.getColumn,strings.getString(s.getSSTIndex).getString)
            case s: LabelRecord => set(s.getRow,s.getColumn,s.getValue)
            case b: BoolErrRecord => if (b.isBoolean) set(b.getRow,b.getColumn,b.getBooleanValue) else throw new FormatFailure("Excel contains a cell error.")
            case f: FormulaRecord =>
              if (f.hasCachedResultString) { set(f.getRow,f.getColumn,null); pendingString = f.getColumn }
              else if (f.getCachedResultType == 3) set(f.getRow,f.getColumn,null)
              else if (f.getCachedResultType == 4) set(f.getRow,f.getColumn,f.getCachedBooleanValue)
              else if (f.getCachedResultType == 5) throw new FormatFailure("Excel contains a cell error.")
              else set(f.getRow,f.getColumn,number(f.getValue.toString))
            case s: StringRecord if pendingString >= 0 => cells(pendingString) = s.getString; pendingString = -1
            case b: BlankRecord => set(b.getRow,b.getColumn,null)
            case b: MulBlankRecord => (b.getFirstColumn.toInt to b.getLastColumn.toInt).foreach(c => set(b.getRow,c,null))
            case _ => ()
          }
        }
      })
      new HSSFEventFactory().processWorkbookEvents(request,fs)
      flush()
    } finally fs.close()
  }
}
