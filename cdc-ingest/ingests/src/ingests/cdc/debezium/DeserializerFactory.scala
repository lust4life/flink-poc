package ingests.cdc.debezium

import com.ververica.cdc.debezium.DebeziumDeserializationSchema

trait DeserializerFactory[T] {
  def get(source: SourceInfo): Option[DebeziumDeserializationSchema[T]]
}
