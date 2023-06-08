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

final case class WithKey[K, T](
    key: K,
    raw: T,
    rawTypeInfo: TypeInformation[T]
)

class ComposedDebeziumDeserializationSchema[K, T: ClassTag](
    deserializerFactory: DeserializerFactory[K, T]
) extends DebeziumDeserializationSchema[WithKey[K, T]] {

  import ComposedDebeziumDeserializationSchema._

  def getProducedType(): TypeInformation[WithKey[K, T]] =
    TypeInformation.of(classOf[WithKey[K, T]])

  def deserialize(
      record: SourceRecord,
      out: Collector[WithKey[K, T]]
  ): Unit = {

    val source = extractSource(record)
    deserializerFactory
      .make(source)
      .foreach {
        case (key, deserializer) => {
          deserializer.deserialize(record, BufferCollector)
          val rawType = deserializer.getProducedType()
          BufferCollector
            .getAndClear()
            .map(WithKey(key, _, rawType))
            .foreach(out.collect)
        }
      }
  }

  private object BufferCollector extends Collector[T] {
    val buffer = ArrayBuffer.empty[T]

    def getAndClear(): Array[T] = {
      val items = buffer.toArray
      buffer.clear()
      items
    }

    def collect(record: T): Unit = buffer += record

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
