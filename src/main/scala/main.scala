package sample.stream

//init: include the namespaces for
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.stream.{ActorFlowMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Source, Sink, FlowGraph, Broadcast, Flow, RunnableFlow, Keep}

object BasicTransformation {

  //! Default main method
  def main(args: Array[String]): Unit = {

    //==============================================================================================

    // create the underlying actor model and materializer to create the streams
    implicit val system = ActorSystem("reactive-tweets")
    implicit val materializer = ActorFlowMaterializer()


    //init: create some text
    val text =
      """|Lorem Ipsum is simply dummy text of the printing and typesetting industry.
         |Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
         |when an unknown printer took a galley of type and scrambled it to make a type
         |specimen book.""".stripMargin

    //==============================================================================================

    // create the source
    val textSource = Source(() => text.split("""\n""").iterator)
    val wordSource = Source(() => text.split(" ").iterator)
    val intSource = Source(1 to 100)

    //==============================================================================================

    // create the pipeline
    val textSource1 = textSource
      .map(_.toUpperCase)
      .filter(line => !line.contains("IPSUM"))

    // run the source with a predefined sink
    textSource1.runForeach(println)

    //==============================================================================================

    // create two different sinks for the console
    val writePrint = Sink.foreach(println)
    val writePrintFilter = Sink.foreach(println)
    // defines a FoldSink
    val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

    // create a flow graph object (allows more complex operations than simple concatenation)
    // note that the FlowGraph is immutable once created
    val g = FlowGraph.closed() { implicit b =>
      // import the operations like ~>
      import FlowGraph.Implicits._

      // define a broadcast, which splits the single toward multiple flows
      val bcast = b.add(Broadcast[Int](2))
      // define the input stream for the broadcast
      intSource ~> bcast.in
      // define the possible outputs (and their sinks)
      bcast.out(0) ~> Flow[Int].map("FULL:   " + _) ~> writePrint
      bcast.out(1) ~> Flow[Int].filter(_ % 2 == 0).map("FILTER: " + _) ~> writePrintFilter
      //bcast.out(2) ~> Flow[Int].map(t => 1).toMat(sumSink)(Keep.right)
    }
    // execute the flow graph object
    g.run()

    //==============================================================================================

    // note that streams can buffer explictly
    intSource
      .buffer(10, OverflowStrategy.dropHead)
      .map(x => x * x)
      .runWith(Sink.ignore)

    //==============================================================================================

    //val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

    import scala.concurrent.ExecutionContext.Implicits.global

    // defines a flow, which is runnable (has a return value)
    // note that this flow is just a blueprint - can be reused multiple times
    val counter: RunnableFlow[Future[Int]] = wordSource.map(t => 1).toMat(sumSink)(Keep.right)
    // execute the stream into a future
    val sum: Future[Int] = counter.run()
    // create a closure to get the result of the Future
    sum.foreach(c => println(s"Total words processed: $c"))

    //==============================================================================================

    val singleSource = Source(1 to 10)
    val singleSink = Sink.fold[Int, Int](0)(_ + _)

    // materialize the flow, getting the Sinks materialized value (SinkFold)
    //val sum1: Future[Int] = singleSource.runWith(singleSink)
    //val sum2: Future[Int] = singleSink.runWith(singleSource)

    val runnable: RunnableFlow[Future[Int]] = Source(1 to 10).toMat(singleSink)(Keep.right)

    // get the materialized value of the FoldSink
    val sum1: Future[Int] = runnable.run()
    val sum2: Future[Int] = runnable.run()


    // print out the value
    sum1.foreach(c => println(s"Total from run 1: $c"))
    sum2.foreach(c => println(s"Total from run 2: $c"))

    //==============================================================================================

    // use different command to create a runnable stream
    // create a source and route it via a flow to a sink
    Source(1 to 6).via(Flow[Int].map(_ * 2)).to(Sink.foreach(println(_)))

    // Starting from a Source
    val source = Source(1 to 6).map(_ * 2)
    // routing the source to a println sink
    source.to(Sink.foreach(println(_)))

    // Starting from a Sink
    val sink: Sink[Int, Unit] = Flow[Int].map(_ * 2).to(Sink.foreach(println(_)))
    // create a source in front of the sink
    Source(1 to 6).to(sink)

    //==============================================================================================

    // An empty source that can be shut down explicitly from the outside
    val source: Source[Int, Promise[Unit]] = Source.lazyEmpty[Int]
    // A flow that internally throttles elements to 1/second, and returns a Cancellable
    // which can be used to shut down the stream
    val flow: Flow[Int, Int, Cancellable] = throttler
    // A sink that returns the first element of a stream in the returned Future
    val sink: Sink[Int, Future[Int]] = Sink.head[Int]

    // By default, the materialized value of the leftmost stage is preserved
    val r1: RunnableFlow[Promise[Unit]] = source.via(flow).to(sink)
    // Simple selection of materialized values by using Keep.right
    val r2: RunnableFlow[Cancellable] = source.viaMat(flow)(Keep.right).to(sink)
    val r3: RunnableFlow[Future[Int]] = source.via(flow).toMat(sink)(Keep.right)

    //==============================================================================================

    // create a partial flow graph (Graph)
    val pickMaxOfThree = FlowGraph.partial() { implicit b =>
      // import the implicits for
      import FlowGraph.Implicits._

      // add two zip with elements to the builder (each zip takes two input elements and return the highest)
      val zip1 = b.add(ZipWith[Int, Int, Int](math.max _))
      val zip2 = b.add(ZipWith[Int, Int, Int](math.max _))
      // connect those elements
      zip1.out ~> zip2.in0

      // define the shape of the partial graph
      // note sources, sinks and flows are special cases (all have a Shape Type)
      // note that source would require to return a single outlet (e.g. zip1.out)
      UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    // create a sink element that returns the first element of the stream
    val resultSink = Sink.head[Int]

    // create the final flow graph (RunnableFlow)
    val g = FlowGraph.closed(resultSink) { implicit b =>
      // the result sink has been passed in
      sink =>
        import FlowGraph.Implicits._

        // importing the partial graph will return its shape (inlets & outlets)
        // note inlets are incoming connection into the partial graph (have a specific type?)
        // outlets are the same for outgoing connections
        val pm3 = b.add(pickMaxOfThree)

        // connect the three sources to the incoming connections
        Source.single(1) ~> pm3.in(0)
        Source.single(2) ~> pm3.in(1)
        Source.single(3) ~> pm3.in(2)
        // connect the outcoming connection to the sink
        pm3.out ~> sink.inlet
    }

    // run the RunnableFlow and wait for the result
    val max: Future[Int] = g.run()
    Await.result(max, 300.millis) should equal(3)

  }
}
