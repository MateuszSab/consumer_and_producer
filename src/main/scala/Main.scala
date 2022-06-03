import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.time.Duration
import java.util._
import scala.jdk.CollectionConverters._

object Main extends App {

  val propsConsumer: Properties = new Properties()
  propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
  propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")


  val propsProducer: Properties = new Properties()
  propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  propsProducer.put("acks", "all")

  val consumer = new KafkaConsumer[String, String](propsConsumer)
  val producer = new KafkaProducer[String, String](propsProducer)
  val topics = Seq("input")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {

      val records = consumer.poll(Duration.ofMillis(100))


      records.asScala.foreach { record =>
        val newrecord = new ProducerRecord[String, String](record.value(), record.key(), record.value().toUpperCase())
        val metadata = producer.send(newrecord)
        printf(s"sent newrecord(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          newrecord.key(), newrecord.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
