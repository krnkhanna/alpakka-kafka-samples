package alpakka.kafka.sample.producer

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future

object ProducerSample extends App {

  val topic = "sample-topic"

  val bootstrapServers = "localhost:29092"

  implicit val system = ActorSystem("alpakka-kafka-sample-producer")

  implicit val executionContext = system.dispatcher

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  val done: Future[Done] =
    Source(1 to 100)
      .map(_.toString)
      .map(value => {
        println(s"producing: $value")
        new ProducerRecord[String, String](topic, value)})
      .runWith(Producer.plainSink(producerSettings))

  done.onComplete(_ => {
    println("************ Message produced ************")
    system.terminate()
  })
}
