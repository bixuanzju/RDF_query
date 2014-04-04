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
      .setSparkHome("/Users/jeremybi/spark-0.9.0-incubating-bin-hadoop1")
    val sc = new SparkContext(conf)

    // val a = sc.parallelize(List((1, "aa"), (2, "bb"), (3, "cc")))
    // val b = sc.parallelize(List((1, "ee"), (2, "ff"), (3, "gg")))
    // val joined = a.join(b).map {
    //   case (key, (value1, value2)) => (key, List(value1, value2))
    // }

    // val c = sc.parallelize(List((1, "gg"), (2, "kk"), (3, "ii")))
    // val joined2 = joined.join(c).map {
    //   case (key, (value1, value2)) => (key, value1 ++ List(value2))
    // }


    // joined2.collect.foreach(println)

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


    query.splan.foreach(splan =>
      splan.bgp_index.foreach(
        index => {
          val bgp = query.newbgp(index)

          val Regex = "<.*#([a-zA-Z]+)>".r
          val pred = bgp.bgp_predicate.split("_").map {
            case Regex(p) => p
          }.reduceLeft(_ + "_" + _)

          superClass(pred.split("_")).foreach(println)

          // bgp.bgp_type match {
          //   case "_PO" => superClass(pred.split("_"), bgp.bgp_type)
          //   case "SP_" => superClass(pred.split("_"), bgp.bgp_type)
          //   case "_P_" => List(pred)
          // }


          // val spo = pred match {
          //   case Regex(sub, pred) => (sub, pred)
          //   case _ => ("noMatch", "noMatch")
          // }

          // val fileNames = superClass(spo)

          // val Regex1 = """\(.*,\((.*),(.*)\)\)""".r
          // val tuples = fileNames.
          //   map(name => sc.textFile(s"hdfs://localhost:9000/user/jeremybi/${spo._2}/$name")).
          //   reduce(_++_).
          //   map {
          //     case Regex1(sub, obj) => (sub, obj)
          //     case _ => ("noMatch", "noMatch")
          //   }

          // tuples.foreach(println)
        }
      )
    )

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

    pred.length match {
      case 1 =>
        List(pred(0))
      case 2 => {

        val lst1 = classes.get(pred(0)) match {
          case None => List(pred(0))
          case Some(lst) => lst
        }
        val lst2 = classes.get(pred(1)) match {
          case None => List(pred(1))
          case Some(lst) => lst
        }

        lst1.map(item1 =>
          lst2.map(item2 => item1 + "_" + item2)
        )
      }
      case 3 => {
        val lst1 = classes.get(pred(0)) match {
          case None => List(pred(0))
          case Some(lst) => lst
        }
        val lst2 = classes.get(pred(1)) match {
          case None => List(pred(1))
          case Some(lst) => lst
        }
        val lst3 = classes.get(pred(2)) match {
          case None => List(pred(2))
          case Some(lst) => lst
        }

        lst1.map(item1 =>
          lst2.map(item2 =>
            lst3.map(item3 => s"${item1}_${item2}_${item3}")
          )
        )
      }
    }
  }
}
