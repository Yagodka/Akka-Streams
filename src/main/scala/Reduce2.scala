import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString

import java.nio.file.Paths

object Reduce2 {

  val FileIn  = "src/main/resources/trans.csv"
  val FileOut = "src/main/resources/result.csv"

  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = sys.dispatcher

  case class Trans(id: String, sum: BigDecimal)

  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime()
    Source
      .fromIterator(() => io.Source.fromFile(FileIn, "utf-8").getLines())
      .drop(1)
      .map(_.split(":").map(_.trim))
      .map{ case (s : Array[String]) => new Trans(s(0), BigDecimal.exact(s(1)))}
      .groupBy(8, _.id)
      .fold(new Trans("", BigDecimal(0))) { (x: Trans, y: Trans) => new Trans(y.id, x.sum.+(y.sum)) }
      .mergeSubstreams
      .map(t => ByteString(s"${t.id} : ${t.sum}\n"))
      .runWith(FileIO.toPath(Paths.get(FileOut)))
      .onComplete(_ => {
        println((System.nanoTime()-t1)/1000000000.0)
        sys.terminate()}
      )
  }
}