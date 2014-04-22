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

    val queryString = " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                      " PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> " +
                      " SELECT ?X ?Y ?Z" +
                      " WHERE " +
                      " { " +
                      "       ?X rdf:type ub:UndergraduateStudent." +
                      "       ?Y rdf:type ub:Department." +
                      "       ?X ub:memberOf ?Y." +
                      "       ?Y ub:subOrganizationOf <http://www.University0.edu>." +
                      "       ?X ub:emailAddress ?Z"+
                      " }"

    val mapPath = "/Users/jeremybi/Desktop/new_data/data/mapping/part-r-00000"

    val lookup = sc.textFile(mapPath).
      map(line => line.split(" ")).
      map(array => (array(0).toLong, (array(1)))).collect.toMap

    val query = new SSEDS(queryString)
    var joined = sc.parallelize(Array((-1L, Vector(-1L)))).cache

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
                (p1.toLong, p2.toLong)
              else
                (p2.toLong, p1.toLong)
            case _ => (-1L, -1L)
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
        //   case (key, vals) => println(lookup(vals(2)))}
        println("Record number is " + joined.count)
      else
        // swap two positions for next join
        if (plan.vars(0) != -1 && plan.vars(1) != -1) joined = joined
      else
        // swap one position for next join
        joined = joined map {
          case (key, vals) => (vals(plan.vars(0)), vals updated (plan.vars(0), key))}
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
