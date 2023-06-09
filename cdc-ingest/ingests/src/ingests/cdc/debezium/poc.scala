package ingests.cdc.debezium

import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.types.DataType
import org.apache.flink.table.data.RowData
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.data.GenericRowData
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import java.util.ArrayList
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path

case class PocFac(
    desirilizerMap: Map[String, DebeziumDeserializationSchema[RowData]]
) extends DeserializerFactory[String, RowData] {
  def make(
      source: SourceInfo
  ): Map[String, DebeziumDeserializationSchema[RowData]] = {
    desirilizerMap
  }

}

object poc {

  def main(args: Array[String]): Unit = {
    val tables = List("orders", "products", "customers")
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
    val tbEnv = StreamTableEnvironment.create(streamEnv)
    tbEnv.executeSql("""
    CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon'
    );
    """)
    tbEnv.getConfig().set("execution.checkpointing.interval", "10 s")
    tbEnv.useCatalog("paimon")

    val desirilizerMap =
      tables
        .map(tbName => {
          val schema = tbEnv.from(tbName).getResolvedSchema()
          val rowType =
            schema
              .toPhysicalRowDataType()
              .getLogicalType()
              .asInstanceOf[RowType]
          schema.toPhysicalRowDataType()

          val resultType = InternalTypeInfo.of(rowType)

          val deserilizer = RowDataDebeziumDeserializeSchema
            .newBuilder()
            .setPhysicalRowType(rowType)
            .setResultTypeInfo(resultType)
            .build()
          (tbName, deserilizer)
        })
        .toMap

    val desirilizer =
      new ComposedDebeziumDeserializationSchema[String, RowData](
        PocFac(desirilizerMap)
      )
    val source = PostgreSQLSource
      .builder()
      .hostname("postgres")
      .port(5432)
      .database("postgres")
      .schemaList("public")
      .tableList("public.orders")
      .username("postgres")
      .password("postgres")
      .decodingPluginName("pgoutput")
      .slotName("orders1")
      // .deserializer(
      //   new JsonDebeziumDeserializationSchema()
      // ) // converts SourceRecord to JSON String
      .deserializer(desirilizer)
      .build()

    val syncStream: DataStream[WithKey[String, RowData]] =
      streamEnv.addSource(source)
    val splited: SingleOutputStreamOperator[RowData] = syncStream
      .forward()
      .process(new ProcessFunction[WithKey[String, RowData], RowData] {
        override def processElement(
            value: WithKey[String, RowData],
            ctx: ProcessFunction[WithKey[String, RowData], RowData]#Context,
            out: Collector[RowData]
        ) = {
          val keyTag = new OutputTag(value.key, value.rawTypeInfo)
          ctx.output(keyTag, value.raw)
          out.collect(value.raw)
        }
      })

    val set = tbEnv.createStatementSet()

    tables.foreach(tbName => {
      val typeInfo = desirilizerMap.get(tbName).get.getProducedType()
      val oneTableStream =
        splited.getSideOutput(
          new OutputTag[RowData](
            tbName,
            typeInfo
          )
        )
      // tbEnv.getConfig().set("pipeline.name", s"poc-ingest-${tbName}")
      tbEnv.createTemporaryView(s"view_${tbName}", oneTableStream)
      // tbEnv.from(s"view-${tbName}").executeInsert(tbName)
      set.addInsert(tbName, tbEnv.from(s"view_${tbName}"))
    })

    tbEnv.getConfig().set("pipeline.name", "ingest")
    set.execute()

    // val sink = StreamingFileSink
    //   .forRowFormat(
    //     new Path("file:///opt/flink/log/test"),
    //     new SimpleStringEncoder[String]("UTF-8")
    //   )
    //   .build()
    // syncStream.map(x => x.key).addSink(sink)
    // streamEnv.execute("ha")
  }
}
