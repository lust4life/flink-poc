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
import com.typesafe.scalalogging.Logger
import org.apache.flink.types.Row
import org.apache.flink.table.api.Schema
import scala.collection.JavaConverters._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.table.data.StringData
object poc {
  val logger = Logger("poc-log")

  def main(args: Array[String]): Unit = {
    // val tables = List("orders", "customers", "products")
    val tables = List("orders")
    // val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()
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
    tbEnv.executeSql("""
    CREATE TABLE if not exists orders (
      order_id STRING,
      product_id STRING,
      customer_id STRING,
      PRIMARY KEY (order_id) NOT ENFORCED
    ) 
    with (
      'changelog-producer' = 'input'
    );
    """)

    val desirilizerMap =
      tables
        .map(tbName => {
          val schema = tbEnv.from(tbName).getResolvedSchema()
          val rowType =
            schema
              .toPhysicalRowDataType()
              .getLogicalType()
              .asInstanceOf[RowType]

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
        new DeserializerFactory[String, RowData] {
          def make(
              source: SourceInfo
          ): Map[String, DebeziumDeserializationSchema[RowData]] = {
            desirilizerMap
          }
        }
      )
    val source = PostgreSQLSource
      .builder()
      .hostname("postgres")
      .port(5432)
      .database("postgres")
      .schemaList("public")
      .tableList(tables.map(name => s"public.${name}"): _*)
      .username("postgres")
      .password("postgres")
      .decodingPluginName("pgoutput")
      .slotName("ingest")
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
          val typeInfo = desirilizerMap.get(value.key).get.getProducedType()
          val keyTag = new OutputTag[RowData](value.key, typeInfo)

          ctx.output(keyTag, value.raw)
          out.collect(value.raw)
        }
      })
      .setParallelism(1)

    val set = tbEnv.createStatementSet()

    tables.foreach(tbName => {
      val typeInfo = desirilizerMap.get(tbName).get.getProducedType()
      val oneTableStream =
        splited
          .getSideOutput(
            new OutputTag[RowData](
              tbName,
              typeInfo
            )
          )

      // tbEnv.createTemporaryView(
      //   s"view_${tbName}",
      //   oneTableStream
      // ) // this isnot work as cdc is a changelog stream
      // set.addInsert(tbName, tbEnv.from(s"view_${tbName}"))
      // tbEnv.getCatalog("").get().getTable(null)

      val fields = tbEnv
        .from(tbName)
        .getResolvedSchema()
        .toPhysicalRowDataType()
        .getLogicalType()
        .asInstanceOf[RowType]
        .getFields()
      val ts = List(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ).map(_.asInstanceOf[TypeInformation[_]]).toArray

      val rowTypeInfo = new RowTypeInfo(
        // fields.asScala
        //   .map(x =>
        //     InternalTypeInfo.of(x.getType()).asInstanceOf[TypeInformation[_]]
        //   )
        //   .toArray,
        ts,
        fields.asScala.map(_.getName()).toArray
      )

      tbEnv
        .fromChangelogStream(
          oneTableStream
            .map(rowData => {
              val arity = rowData.getArity
              val newRow = Row.withPositions(rowData.getRowKind(), arity)

              (0 until arity).foreach { i =>
                val field = rowData.asInstanceOf[GenericRowData].getField(i)
                logger.info(s"type $i is ===> ${field.getClass()}")
                logger.info(s"value $i is ===> ${field}")
                newRow.setField(i, field.toString())
              }
              newRow
            })
            .returns(rowTypeInfo),
          Schema
            .newBuilder()
            .fromResolvedSchema(
              tbEnv.from(tbName).getResolvedSchema()
            )
            // .fromFields(
            //   rowTypeInfo.getFieldNames(),
            //   List(
            //     DataTypes.STRING(),
            //     DataTypes.STRING(),
            //     DataTypes.STRING(),
            //     DataTypes.TIMESTAMP_LTZ()
            //   ).toArray[AbstractDataType[_]]
            // )
            .build()
        )
        .executeInsert(tbName)

      // tbEnv.getConfig().set("pipeline.name", s"poc-ingest-${tbName}")
      // tbEnv.from(s"view-${tbName}").executeInsert(tbName) // one job per table
    })

    // tbEnv.getConfig().set("pipeline.name", "ingest")
    // set.execute() // one job all tables

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
