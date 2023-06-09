package test.utest.examples
import ingests.cdc.debezium._
import utest._

class A {

  object B {}

}

object HelloTests extends TestSuite {
  val tests = Tests {
    test("poc") {
      poc.main(Array.empty)
    }
  }
}
