package dacp

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Types}
import java.util.Properties
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.{DateUnit, TimeUnit, FloatingPointPrecision}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

case class SqlConfig(url: String, user: Option[String] = None, password: Option[String] = None,
                     schema: Option[String] = None, tables: Option[Seq[String]] = None,
                     fetchSize: Int = 1024, queryTimeoutSeconds: Int = 60) {
  require(url.startsWith("jdbc:"), "SQL dataset URL must be JDBC")
  require(fetchSize > 0 && queryTimeoutSeconds > 0)
  require(tables.forall(_.forall(SqlAdapter.validName)), "invalid SQL table name")
  // JDBC URLs/passwords must never appear in case-class logs or error responses.
  override def toString: String = "SqlConfig(<redacted>)"
}

/** Private JDBC reader for sqldb/custom providers; public aliases are resolved before calling it. */
class SqlAdapter(config: SqlConfig, batchSize: Int) extends DataSetAdapter {
  require(batchSize > 0)
  private def connection(): Connection = {
    val props = new Properties()
    config.user.foreach(props.setProperty("user",_)); config.password.foreach(props.setProperty("password",_))
    val c = DriverManager.getConnection(config.url,props)
    try {
      c.setReadOnly(true)
      // PostgreSQL requires a transaction plus positive fetch size for server-side cursors.
      c.setAutoCommit(false)
      c
    } catch { case t: Throwable => c.close(); throw t }
  }
  private def withConnection[A](f: Connection => A): A = {
    val c = connection()
    try f(c) finally c.close() // Closing rolls back the read transaction, including failed/cancelled reads.
  }
  private def schemaName(c: Connection): String = config.schema.getOrElse(c.getSchema)
  private def tableNames(c: Connection): Seq[String] = {
    val schema = schemaName(c)
    require(schema != null && schema.nonEmpty,"SQL schema must be configured for this driver")
    // Filter exact schema ourselves: JDBC metadata schema arguments are LIKE patterns.
    val rs = c.getMetaData.getTables(c.getCatalog,null,"%",Array("TABLE", "BASE TABLE"))
    try {
      val names = Vector.newBuilder[String]
      while(rs.next()) {
        val name = rs.getString("TABLE_NAME")
        if (rs.getString("TABLE_SCHEM") == schema && SqlAdapter.validName(name) && config.tables.forall(_.contains(name))) names += name
      }
      names.result().distinct.sorted
    } finally rs.close()
  }
  private def qualified(c: Connection, item: String): String = {
    require(SqlAdapter.validName(item),"invalid SQL table name")
    if (!tableNames(c).contains(item)) throw new java.io.FileNotFoundException("SQL table not found: " + item)
    val quote = c.getMetaData.getIdentifierQuoteString.trim
    require(quote.nonEmpty,"JDBC driver must support quoted identifiers")
    def q(s: String): String = quote + s.replace(quote,quote+quote) + quote
    q(schemaName(c)) + "." + q(item)
  }
  private def describe(c: Connection, item: String): Item = {
    val name = qualified(c,item)
    val statement = c.createStatement()
    try {
      statement.setQueryTimeout(config.queryTimeoutSeconds)
      val rs = statement.executeQuery(s"SELECT * FROM $name WHERE 1=0")
      val schema = try SqlAdapter.schema(rs.getMetaData) finally rs.close()
      Item(item,"sql",Some(schema))
    } finally statement.close()
  }
  def count(item: String): Long = withConnection { c =>
    val name = qualified(c,item)
    val statement = c.createStatement()
    try {
      statement.setQueryTimeout(config.queryTimeoutSeconds)
      val result = statement.executeQuery(s"SELECT COUNT(*) FROM $name")
      try { require(result.next(), "record count unavailable"); result.getLong(1) }
      finally result.close()
    } finally statement.close()
  }
  def describe(item: String): Item = withConnection(c => describe(c,item))
  def list(): Seq[Item] = withConnection(c => tableNames(c).map(describe(c,_)))

  // At most one Arrow batch plus the JDBC fetch window is buffered; no retries or materialization.
  def stream(item: String, allocator: BufferAllocator)(start: VectorSchemaRoot => Unit,
             emit: VectorSchemaRoot => Unit, ready: () => Unit): Unit = withConnection { c =>
    val name = qualified(c,item)
    val st = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)
    try {
      st.setFetchSize(config.fetchSize); st.setQueryTimeout(config.queryTimeoutSeconds)
      ready()
      val rs = st.executeQuery(s"SELECT * FROM $name")
      try {
        val root = VectorSchemaRoot.create(SqlAdapter.schema(rs.getMetaData),allocator)
        try {
          start(root)
          var done = false
          while (!done) {
            ready(); root.clear(); root.allocateNew()
            var row = 0
            while (row < batchSize && !done) {
              if (!rs.next()) done = true
              else {
                root.getFieldVectors.asScala.zipWithIndex.foreach { case (v,col) => SqlAdapter.set(v,row,rs,col+1) }
                row += 1
              }
            }
            if (row > 0) { root.setRowCount(row); emit(root) }
          }
        } finally root.close()
      } finally rs.close()
    } finally st.close()
  }
}
object SqlAdapter {
  def validName(s: String): Boolean = s.nonEmpty && !s.contains("/") && !s.contains("\\") && s != "." && s != ".." && !s.exists(_.isControl)
  def schema(meta: ResultSetMetaData): Schema = {
    val fields = (1 to meta.getColumnCount).map { i =>
      val t: ArrowType = meta.getColumnType(i) match {
        case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT => new ArrowType.Int(64,true)
        case Types.REAL | Types.FLOAT | Types.DOUBLE => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        case Types.BOOLEAN | Types.BIT => new ArrowType.Bool()
        case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR => new ArrowType.Utf8()
        case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => new ArrowType.Binary()
        case Types.NUMERIC | Types.DECIMAL =>
          val precision = meta.getPrecision(i); val scale = meta.getScale(i)
          require(precision > 0 && precision <= 38 && scale >= 0 && scale <= precision,"unsupported SQL decimal precision/scale")
          new ArrowType.Decimal(precision,scale,128)
        case Types.DATE => new ArrowType.Date(DateUnit.DAY)
        case Types.TIME => new ArrowType.Time(TimeUnit.MICROSECOND,64)
        case Types.TIMESTAMP => new ArrowType.Timestamp(TimeUnit.MICROSECOND,null)
        case Types.TIMESTAMP_WITH_TIMEZONE => new ArrowType.Timestamp(TimeUnit.MICROSECOND,"UTC")
        case _ => throw new IllegalArgumentException(s"unsupported SQL type for column ${meta.getColumnLabel(i)}: ${meta.getColumnTypeName(i)}")
      }
      new Field(meta.getColumnLabel(i),new FieldType(meta.isNullable(i) != ResultSetMetaData.columnNoNulls,t,null),null)
    }
    new Schema(fields.asJava)
  }
  private def micros(t: java.time.Instant): Long = Math.addExact(Math.multiplyExact(t.getEpochSecond,1000000L),t.getNano / 1000)
  def set(v: FieldVector, row: Int, rs: ResultSet, col: Int): Unit = {
    if (rs.getObject(col) == null) { v.setNull(row); return }
    v match {
      case x: BigIntVector => x.setSafe(row,rs.getLong(col))
      case x: Float8Vector => x.setSafe(row,rs.getDouble(col))
      case x: BitVector => x.setSafe(row,if(rs.getBoolean(col)) 1 else 0)
      case x: VarCharVector => x.setSafe(row,rs.getString(col).getBytes(java.nio.charset.StandardCharsets.UTF_8))
      case x: VarBinaryVector => x.setSafe(row,rs.getBytes(col))
      case x: DecimalVector => x.setSafe(row,rs.getBigDecimal(col))
      case x: DateDayVector => x.setSafe(row,Math.toIntExact(rs.getObject(col,classOf[java.time.LocalDate]).toEpochDay))
      case x: TimeMicroVector => x.setSafe(row,rs.getObject(col,classOf[java.time.LocalTime]).toNanoOfDay / 1000)
      case x: TimeStampMicroTZVector => x.setSafe(row,micros(rs.getObject(col,classOf[java.time.OffsetDateTime]).toInstant))
      case x: TimeStampMicroVector => x.setSafe(row,micros(rs.getObject(col,classOf[java.time.LocalDateTime]).toInstant(java.time.ZoneOffset.UTC)))
      case _ => throw new IllegalArgumentException("unsupported SQL Arrow vector")
    }
  }
  /** Tables have no original file bytes. Their byte view is an Arrow IPC stream, chunked as binary batches. */
  def bytes(sql: SqlAdapter, item: String, allocator: BufferAllocator, chunkSize: Int,
            listener: ServerStreamListener, ready: () => Unit): Unit = {
    val root = VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
    val out = new java.io.OutputStream {
      val buffer = new Array[Byte](chunkSize); var used = 0
      override def write(b: Int): Unit = { buffer(used) = b.toByte; used += 1; if(used == buffer.length) flush() }
      override def write(b: Array[Byte], offset: Int, length: Int): Unit = {
        var pos = offset; var left = length
        while(left > 0) { val n = math.min(left,buffer.length-used); System.arraycopy(b,pos,buffer,used,n)
          used += n; pos += n; left -= n; if(used == buffer.length) flush() }
      }
      override def flush(): Unit = if(used > 0) {
        ready(); root.clear(); root.allocateNew()
        root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,buffer,0,used)
        root.setRowCount(1); listener.putNext(); used = 0
      }
    }
    var writer: ArrowStreamWriter = null
    try {
      listener.start(root)
      sql.stream(item,allocator)(r => { writer = new ArrowStreamWriter(r,null,out); writer.start() },_ => writer.writeBatch(),ready)
      writer.end(); out.flush()
    } finally { try { if(writer != null) writer.close() } finally root.close() }
  }
}
