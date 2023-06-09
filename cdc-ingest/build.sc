import $ivy.`com.lihaoyi::mill-contrib-scoverage:$MILL_VERSION`
import mill._
import mill.api.Loose
import mill.define.Target
import scalalib._
import mill.contrib.scoverage.ScoverageModule

object Deps {
  def scalaReflect(scalaVersion: String) =
    ivy"org.scala-lang:scala-reflect:${scalaVersion}"

  val utest = ivy"com.lihaoyi::utest::0.8.1"
  val mockito = ivy"org.mockito::mockito-scala:1.17.14"
}

object ingests extends ScoverageModule {
  override def scalaVersion = "2.12.18"

  override def scoverageVersion = "2.0.10"

  def flinkVersion = "1.16.0"
  def flinkCDCVersion = "2.3.0"
  def debeziumVersion = "2.2.1.Final"

  override def ivyDeps: Target[Loose.Agg[Dep]] = Agg(
    // ivy"io.debezium:debezium-api:${debeziumVersion}",
    // ivy"io.debezium:debezium-embedded:${debeziumVersion}",
    // ivy"io.debezium:debezium-connector-postgres:${debeziumVersion}",
    // ivy"com.ververica:flink-connector-debezium:${flinkCDCVersion}",
    ivy"com.ververica:flink-connector-postgres-cdc:${flinkCDCVersion}",
    // ivy"org.apache.flink:flink-core:${flinkVersion}",
    // ivy"org.apache.flink:flink-table-common:${flinkVersion}",
    // ivy"org.apache.flink:flink-table-runtime:${flinkVersion}",
    // ivy"org.apache.flink:flink-table-api-java-bridge:${flinkVersion}",
    ivy"com.lihaoyi::os-lib:0.7.0",
    Deps.scalaReflect(scalaVersion())
  )

  def compileIvyDeps = Agg(
    ivy"org.apache.flink:flink-table-runtime:${flinkVersion}"
  )

  def runIvyDeps = Agg(
    ivy"org.apache.paimon:paimon-flink-1.16:0.4.0-incubating",
    ivy"org.apache.hadoop:hadoop-common:3.3.5"
  )

  trait utest extends ScoverageTests with TestModule.Utest {
    override def moduleDeps: Seq[JavaModule] = super.moduleDeps ++ Seq(ingests)

    override def ivyDeps = Agg(
      Deps.utest,
      Deps.mockito
    )
  }

  object test extends utest

  object integration extends utest

}
