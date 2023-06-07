package ingests.cdc.debezium

import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import com.ververica.cdc.debezium.table._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.table.data.RowData
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.kafka.connect.data.Struct
import io.debezium.data.Envelope
import io.debezium.connector.AbstractSourceInfo

final case class SourceInfo(
    db: String,
    schema: String,
    table: String,
    ts_ms: Long
)

final case class WithSource[T](
    source: SourceInfo,
    raw: T
)

class ComposedDebeziumDeserializationSchema[T: ClassTag](
    deserializerFactory: DeserializerFactory[T]
) extends DebeziumDeserializationSchema[WithSource[T]] {

  import ComposedDebeziumDeserializationSchema._

  def getProducedType(): TypeInformation[WithSource[T]] =
    TypeInformation.of(classOf[WithSource[T]])

  def deserialize(
      record: SourceRecord,
      out: Collector[WithSource[T]]
  ): Unit = {

    val source = extractSource(record)
    deserializerFactory
      .get(source)
      .foreach(deserializer => {
        deserializer.deserialize(record, BufferCollector)
        BufferCollector
          .getAndClear()
          .map(WithSource(source, _))
          .foreach(out.collect)
      })
  }

  private object BufferCollector extends Collector[T] {
    val buffer = ArrayBuffer.empty[T]

    def getAndClear(): Array[T] = {
      val items = buffer.toArray
      buffer.clear()
      items
    }

    def collect(record: T): Unit = buffer.addOne(record)

    def close(): Unit = {}
  }
}

object ComposedDebeziumDeserializationSchema {

  def extractSource(record: SourceRecord): SourceInfo = {
    val msg = record.value().asInstanceOf[Struct]
    val source = msg.getStruct(Envelope.FieldName.SOURCE)
    val db = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY)
    val schema = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY)
    val table = source.getString(AbstractSourceInfo.TABLE_NAME_KEY)
    val ts_ms = source.getInt64(AbstractSourceInfo.TIMESTAMP_KEY)

    SourceInfo(db, schema, table, ts_ms)
  }

}
