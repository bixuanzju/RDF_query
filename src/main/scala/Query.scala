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
      .setJars(List("target/scala-2.10/query-project_2.10-1.0.jar"))
      .setSparkHome("/Users/jeremybi/spark-0.9.1-bin-hadoop1")
    val sc = new SparkContext(conf)

    val mapPath = "/Users/jeremybi/Desktop/new_data/data/mapping/part-r-00000"
    val lookup = sc.textFile(mapPath).
      map(line => line.split(" ")).
      map(array => (array(0).toLong, (array(1)))).collect.toMap

    val queryString = new QueryString()
    val query = new SSEDS(QueryString.q(1))
    var joined = sc.parallelize(Array(((-1L, -1L), Vector(-1L)))).cache

    for (i <- 0 until query.qplan.length) {
      val plan = query.qplan(i)

      for (index <- plan.bgp_index) {
        val bgp = query.newbgp(index)

        // TODO: drop superClass for the moment
        val fileName = "ff" + bgp.bgp_predicate_id

        val Regex2 = """\((-?\d+),(-?\d+)\)""".r

        // swap positions for this join
        val tuples = sc.textFile("hdfs://localhost:9000/user/jeremybi/partitions/" + fileName).
          map {
            case Regex2(p1, p2) =>
              if (plan.name == bgp.bgp_var(0))
                ((p1.toLong, -1L), p2.toLong)
              else if (plan.name.length == 2)
                ((p1.toLong, p2.toLong), -1L)
              else ((p2.toLong, -1L), p1.toLong)
            case _ => ((-1L, -1L), -1L)
          }.
          filter {
            case (_, obj) =>
              if (bgp.bgp_type == "_PO") obj == bgp.bgp_object_id.toLong
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
      if (i == query.qplan.length - 1)
        // joined.collect.foreach {
          // case (key, vals) => println(lookup(key))}
        println("Record number is " + joined.count)
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
    val classes = Map(("Professor" -> List("AssociateProfessor", "FullProfessor",
                                           "AssistantProfessor")),
                      ("Person" -> List("FullProfessor", "AssociateProfessor",
                                        "AssistantProfessor", "Lecturer",
                                        "UndergraduateStudent", "GraduateStudent",
                                        "TeachingAssistant", "ResearchAssistant")),
                      ("Student" -> List("GraduateStudent", "UndergraduateStudent")),
                      ("Faculty" -> List("FullProfessor", "AssociateProfessor",
                                         "AssistantProfessor", "Lecturer")),
                      ("Chair" -> List("FullProfessor", "AssociateProfessor",
                                       "AssistantProfessor")))

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
