package test.utest.examples
import ingests.cdc.debezium._
import utest._

class A {

  object B {}

}

object HelloTests extends TestSuite {
  val tests = Tests {
    test("test1") {
      val c = new A().B
      val d = new A().B
      assert(c == d)
    }
  }
}
