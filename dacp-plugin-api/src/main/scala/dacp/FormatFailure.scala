package dacp

/** Fixed public message; private backend diagnostics must not be included. */
class FormatFailure(message: String) extends RuntimeException(message)
