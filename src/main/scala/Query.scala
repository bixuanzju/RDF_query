import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import common._

object Query {
  def main(args: Array[String]) = {

    // Initialization
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Exercise")
      .setJars(SparkContext.jarOfClass(this.getClass))
      .setSparkHome(System.getenv("SPARK_HOME"))
      .set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)

    val typeHash = "-1425683616493199"

    // val mapPath = "/Users/jeremybi/Desktop/new_data/data/mapping/part-r-00000"
    // val lookup = sc.broadcast(sc.textFile(mapPath).
    //                             map(line => line.split(" ")).
    //                             map(array => (array(0).toLong, array(1))).collect.toMap)

    val namePath = "hdfs://192.168.13.200:9000/user/root/partitions/filenames"
    val filePaths = sc.broadcast(sc.textFile(namePath).collect.toSet)


    val queryNum = args(0).toInt - 1
    val _ = new QueryString()
    val query = new SSEDS(QueryString.q(queryNum))


    var joined = sc.parallelize(Array(((-1L, -1L), Vector(-1L))))

    for (i <- 0 until query.qplan.length) {
      val plan = query.qplan(i)

      for (index <- plan.bgp_index) {
        val bgp = query.newbgp(index)

        val fileName =
          superClass((if (bgp.bgp_predicate_id == typeHash)
                        bgp.bgp_object_id
                      else bgp.bgp_predicate_id).
                       split("_")).map("ff" + _).
            filter(filePaths.value contains _)

        val Regex1 = """\((-?\d+),(-?\d+)\)""".r
        val Regex2 = """(-?\d+)""".r

        // swap positions for this join
        val tuples = fileName.map(name =>
          sc.textFile("hdfs://192.168.13.200:9000/user/root/partitions/" + name)).
          reduceLeft(_ ++ _).
          map {
            case Regex1(p1, p2) =>
              if (plan.name == bgp.bgp_var(0))
                ((p1.toLong, -1L), p2.toLong)
              else if (plan.name.length == 2)
                ((p1.toLong, p2.toLong), -1L)
              else ((p2.toLong, -1L), p1.toLong)
            case Regex2(p) => ((p.toLong, -1L), -1L)
            case _ => ((-1L, -1L), -1L)
          }.
          filter {
            case (_, obj) =>
              if (bgp.bgp_type == "_PO" && bgp.bgp_predicate_id != typeHash)
                obj == bgp.bgp_object_id.toLong
              else if (bgp.bgp_type == "SP_") obj == bgp.bgp_subject_id.toLong
              else true
          } // TODO: partitionBy


        if (index == 0)
          joined = tuples.map {
            case (key, value) => (key, Vector(value))}
        else
          joined = joined.join(tuples).mapValues {
            case (vals, value) => (vals :+ value)}
      }

      // output
      if (i == query.qplan.length - 1) {
        // joined.collect.foreach {
        //   case (key, vals) => println(key._1)}
        println("Record number is " + joined.count)
      }
      else
        // swap two positions for next join
        if (plan.vars(0) != -1 && plan.vars(1) != -1)
        joined = joined map {
          case (key, vals) => ((vals(plan.vars(0)), vals(plan.vars(1))),
                                vals updated (plan.vars(0), key._1) updated (plan.vars(1), -1L))
        }
        else
          // swap one position for next join
          joined = joined map {
            case (key, vals) => ((vals(plan.vars(0)), -1L), vals updated (plan.vars(0), key._1))}
    }

    sc.stop()

  }

  def superClass(pred : Array[String]) = {
    // val classes = Map(("Professor" -> List("AssociateProfessor", "FullProfessor",
    //                                        "AssistantProfessor")),
    //                   ("Person" -> List("FullProfessor", "AssociateProfessor",
    //                                     "AssistantProfessor", "Lecturer",
    //                                     "UndergraduateStudent", "GraduateStudent",
    //                                     "TeachingAssistant", "ResearchAssistant")),
    //                   ("Student" -> List("GraduateStudent", "UndergraduateStudent")),
    //                   ("Faculty" -> List("FullProfessor", "AssociateProfessor",
    //                                      "AssistantProfessor", "Lecturer")),
    //                   ("Chair" -> List("FullProfessor", "AssociateProfessor",
    //                                    "AssistantProfessor")))
    val classes = Map(("-198794852858" -> List("203362154867", "-182999052962",
                                               "22271127667")),
                      ("175814932055" -> List("-182999052962", "203362154867",
                                              "22271127667", "-9409035957",
                                              "137572752569", "-198122920164",
                                              "-102439739466", "106659238066")),
                      ("10090957256" -> List("-198122920164", "137572752569")),
                      ("-126289946156" -> List("-182999052962", "203362154867",
                                         "22271127667", "-9409035957")),
                      ("162645273054" -> List("-182999052962", "203362154867",
                                       "22271127667")))

    pred.foldLeft(Nil: List[String])(
      (lst, item) => {
        val candidates = classes.get(item) match {
          case None => List(item)
          case Some(cand) => cand
        }
        if (lst.isEmpty)
          candidates
        else
          for {
            item1 <- lst
            item2 <- candidates
          } yield item1 + "_" + item2})
  }
}
