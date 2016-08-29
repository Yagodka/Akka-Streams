import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import java.nio.file.Paths

object Reduce {

  val FileIn = "src/main/resources/trans.csv"
  val FileOut = "src/main/resources/result.csv"

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  case class Transaction(id: String, sum: BigDecimal){
    override def toString: String = s"$id : $sum"
  }

  def main(args: Array[String]): Unit = {

    val csvLines: Iterator[String] = io.Source.fromFile(FileIn, "utf-8").getLines()

    def stringArrayToTransactions(cols: Array[String]) = new Transaction(cols(0), BigDecimal.exact(cols(1)))

    val csvToTrans = Flow[String]
      .drop(1)
      .map(_.split(":").map(_.trim))
      .map(stringArrayToTransactions)

    val summarizeTrans = Flow[Transaction]
        .groupBy(30, _.id)
        .fold(new Transaction("", BigDecimal(0))) {
          (x: Transaction, y: Transaction) =>
            val sumById = x.sum.+(y.sum)
            new Transaction(y.id, sumById)
        }.mergeSubstreams

    val transToCvs = Flow[Transaction]
      .map(t => {
        println(t.toString)
        ByteString(t.toString + "\n")
      })

    def lineSink(): Sink[ByteString, Future[IOResult]] = Flow[ByteString]
      .toMat(FileIO.toPath(Paths.get(FileOut)))(Keep.right)

    val g = {
      RunnableGraph.fromGraph(GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

            val A: Outlet[String]                      = builder.add(Source.fromIterator(() => csvLines)).out
            val B: FlowShape[String, Transaction]      = builder.add(csvToTrans)
            val C: FlowShape[Transaction, Transaction] = builder.add(summarizeTrans)
            val D: FlowShape[Transaction, ByteString]  = builder.add(transToCvs)
            val E: Inlet[ByteString]                   = builder.add(lineSink).in

          A ~> B ~> C ~> D ~> E

          ClosedShape
      })
    }.run()
  }
}