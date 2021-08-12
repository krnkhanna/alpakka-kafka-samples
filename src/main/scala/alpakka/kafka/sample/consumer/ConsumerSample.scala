package alpakka.kafka.sample.consumer

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

object ConsumerSample extends App {

  val topic = "sample-topic"

  val bootstrapServers = "localhost:29092"

  implicit val system = ActorSystem("alpakka-kafka-sample-consumer")

  implicit val executionContext = system.dispatcher

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val done: Future[Done] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
      .map(consumerRecord => consumerRecord.value)
      .runWith(Sink.foreach(message => println(s"consumed: $message")))

  done.onComplete(_ => {
    println("************ Message consumed ************")
    system.terminate()
  })
}
