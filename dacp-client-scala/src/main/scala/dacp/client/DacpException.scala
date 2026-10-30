package dacp.client


import java.io.IOException
import org.apache.arrow.flight.FlightRuntimeException

/** Public DACP failure; transport diagnostics are deliberately omitted. */
final class DacpException(val code: String, message: String) extends IOException(message)

private[client] object DacpErrors {
  def translate(error: Throwable): Throwable = error match {
    case e: FlightRuntimeException =>
      val detail = Option(e.status().description()).getOrElse("").toLowerCase(java.util.Locale.ROOT)
      val status = e.status().code().toString
      val (code, message) =
        if (detail.contains("dataset not found")) ("NOT_FOUND", "Dataset not found. Check the dataset name.")
        else if (Set("UNAUTHORIZED", "UNAUTHENTICATED")(status)) ("ACCESS_DENIED", "Access denied. Check your credentials and permissions.")
        else if (status == "NOT_FOUND") ("NOT_FOUND", "Item or dataset not found. Check the name and path.")
        else if (status == "TIMED_OUT") ("TIMEOUT", "The request timed out. Try again later.")
        else if (status == "UNAVAILABLE") ("UNAVAILABLE", "Cannot reach the DACP server. Check the address and connection.")
        else if (status == "CANCELLED") ("CANCELLED", "The request was cancelled.")
        else if (detail.contains("scan_error")) ("INVALID_ARGUMENT", "Invalid records query. Check column names, literal types, projection, and limit.")
        else if (detail.contains("format_error")) ("FORMAT_ERROR", "Cannot read this data format. Check the file, sheet selection, and format settings.")
        else if (detail.contains("schema_conflict")) ("SCHEMA_CONFLICT", "The data does not match its declared schema.")
        else if (detail.contains("folder has no content stream")) ("INVALID_ARGUMENT", "Folders have no readable content. Use list(path) to browse them.")
        else if (detail.contains("listing requires a folder")) ("INVALID_ARGUMENT", "Only folders can be listed.")
        else if (detail.contains("unsupported stream view")) ("INVALID_ARGUMENT", "This item does not support the requested view.")
        else ("REQUEST_FAILED", "The DACP request failed. Try again or contact the server administrator.")
      new DacpException(code, message)
    case other => other
  }
  def protect[A](body: => A): A = try body catch {
    case e: FlightRuntimeException => throw translate(e)
  }
}
