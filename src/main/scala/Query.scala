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
                        " PREFIX ub: <http://www.lehigh.edu/hp2/2004/0401/univ-bench.owl#> " +
                        " SELECT ?X ?Y ?Z" +
                        " WHERE " +
                        " { " +
                        "       ?X rdf:type ub:Student." +
                        "       ?Y rdf:type ub:Department." +
                        "       ?X ub:memberOf ?Y." +
                        "       ?Y ub:subOrganizationOf <http://www.University0.edu>." +
                        "       ?X ub:emailAddress ?Z"+
                        " }"

    val query = new SSEDS(queryString)
    var joined = sc.parallelize(Array(("", Vector("")))).cache

    query.splan.foreach(
      splan => {
        splan.bgp_index.foreach(
          index => {
            val bgp = query.newbgp(index)

            val Regex1 = """<.*#([a-zA-Z]+)>""".r
            val pred = bgp.bgp_predicate.split("_").map {
              case Regex1(p) => p
            }.reduceLeft(_ + "_" + _)

            val fileNames = superClass(pred.split("_"))

            val Regex2 = """\(.*,\((.*),(.*)\)\)$""".r
            val tuples = fileNames.
              map(file => sc.textFile("hdfs://localhost:9000/user/jeremybi/" + file)).
              reduceLeft(_ ++ _).
              map {
                case Regex2(p1, p2) =>
                  if (splan.name == bgp.bgp_var(0))
                    (p1, p2)
                  else
                    (p2, p1)
                case _ => ("noMatch", "noMatch")}.
              partitionBy(new HashPartitioner(8))


            if (index == 0)
              joined = tuples.map {
                case (key, value) => (key, Vector(value))}
            else
              joined = joined.join(tuples).mapValues {
                case (vals, value) => (vals :+ value)}})

        // swap position for next join
        if (splan.vars(0) == "")
          joined.collect.foreach {
            case (key, vals) => println(key)
          }
          // println("Total number is " + joined.collect.length)
        else
          joined = joined map {
            case (key, vals) => (vals(1), vals updated (1, key))
          }
      }
    )

    // val lines1 = sc.textFile("/Users/jeremybi/Desktop/Q8.txt").collect
    // val lines2 = sc.textFile("/Users/jeremybi/Desktop/Output.txt").collect

    // lines1.filter(line => !(lines2 contains line)).foreach(println)

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
