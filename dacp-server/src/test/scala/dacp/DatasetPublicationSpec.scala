package dacp

import java.time.Instant
import org.apache.arrow.flight.FlightRuntimeException
import org.scalatest.funsuite.AnyFunSuite

class DatasetPublicationSpec extends AnyFunSuite {
  private def description(policy:String)=s"""{"title":"Example","description":"Public metadata","publication":$policy}"""
  test("policies are dataset-wide, strict and default to private") {
    assert(!DatasetPublication.parse(_root_.xstream.data.DatasetDescriptions.default()).readable())
    assert(DatasetPublication.parse(description("""{"policy":"public"}""")).readable())
    assert(!DatasetPublication.parse(description("""{"policy":"private"}""")).readable())
    Seq("null","{}","""{"policy":"restricted"}""","""{"policy":"public","public_at":"2027-01-01T00:00:00Z"}""","""{"policy":"embargoed","public_at":"tomorrow"}""").foreach { bad=>
      intercept[Exception](DatasetPublication.parse(description(bad)))
    }
  }
  test("embargo expires exactly at the specified UTC instant") {
    val at=Instant.parse("2026-12-01T00:00:00Z")
    val embargo=DatasetPublication.parse(description("""{"policy":"embargoed","public_at":"2026-12-01T00:00:00Z"}"""))
    assert(!embargo.readable(at.minusNanos(1)));assert(embargo.readable(at));assert(embargo.readable(at.plusSeconds(1)))
  }
  test("only explicitly trusted Union service admission bypasses content policy") {
    val privateData=description("""{"policy":"private"}""")
    DatasetPublication.requireRead(privateData,trusted=true)
    intercept[FlightRuntimeException](DatasetPublication.requireRead(privateData))
    DatasetPublication.requireRead(description("""{"policy":"public"}"""))
  }
}
