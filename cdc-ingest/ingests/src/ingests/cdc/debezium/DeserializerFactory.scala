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
import ingests.cdc.debezium.ComposedDebeziumDeserializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

trait DeserializerFactory[K, T] {
  def make(source: SourceInfo): Map[K, DebeziumDeserializationSchema[T]]
}

object poc {

  def main(args: Array[String]): Unit = {
    val tables = List("orders", "products", "customers")
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
    val tbEnv = StreamTableEnvironment.create(streamEnv)

    val desirilizerMap =
      tables
        .map(tbName => {
          val schema = tbEnv.from(tbName).getResolvedSchema()
          val rowType =
            schema
              .toPhysicalRowDataType()
              .getLogicalType()
              .asInstanceOf[RowType]
          val resultType = TypeInformation
            .of(classOf[DataType])
            .asInstanceOf[TypeInformation[RowData]]
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
        new DeserializerFactory[String, RowData] {
          def make(
              source: SourceInfo
          ): Map[String, DebeziumDeserializationSchema[RowData]] = {
            desirilizerMap.get(source.table).map((source.table, _)).toMap
          }
        }
      )
    val source = PostgreSQLSource
      .builder()
      .hostname("localhost")
      .database("postgres")
      .schemaList("public")
      .tableList(tables: _*)
      .username("postgres")
      .password("postgres")
      .decodingPluginName("pgoutput")
      .slotName("test")
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
          val keyTag = new OutputTag[RowData](value.key)
          ctx.output(keyTag, value.raw)
          out.collect(value.raw)
        }
      })

    tables.foreach(tbName => {
      val oneTableStream = splited.getSideOutput(new OutputTag[RowData](tbName))
      tbEnv.fromDataStream(oneTableStream).insertInto(tbName)
    })

    streamEnv.execute()
  }
}
