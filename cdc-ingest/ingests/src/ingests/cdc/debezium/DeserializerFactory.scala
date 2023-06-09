package ingests.cdc.debezium

import com.ververica.cdc.debezium.DebeziumDeserializationSchema

trait DeserializerFactory[K, T] extends Serializable {
  def make(source: SourceInfo): Map[K, DebeziumDeserializationSchema[T]]
}
