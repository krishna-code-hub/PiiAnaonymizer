package com.krishna.piianonymization



import scala.language.implicitConversions
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Date
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, CoreDocument, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.sql.functions.{col, expr, udf}

import java.util.Properties
import java.util.regex.Pattern


object PiiAnonymizer {

  var props = new Properties()
  props.put("annotators", "tokenize, ssplit, pos, lemma, ner, regexner")

  val pipeline = new StanfordCoreNLP(props)

  def anonymize(colString: String): String = {
    val replacementString = "ANONYMIZED_VALUE"

    val annotation = new Annotation(colString)

    pipeline.annotate(annotation)

    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])

    import scala.collection.JavaConversions._

    var text = colString

    for (sentence <- sentences) {

      // Traversing the words in the current sentence
      // A CoreLabel is a CoreMap with additional token-specific methods

      //import scala.collection.JavaConversions._

      for (token <- sentence.get(classOf[CoreAnnotations.TokensAnnotation])) {
        println("token.ner = " + token.ner())
        println("token.word = " + token.word())
        if (token.ner.equalsIgnoreCase("PERSON") ||
          token.ner.equalsIgnoreCase("LOCATION") ||
          token.ner.equalsIgnoreCase("ORGANIZATION") ||
          token.ner.equalsIgnoreCase("EMAIL") ||
          token.ner.equalsIgnoreCase("CITY") ||
          token.ner.equalsIgnoreCase("COUNTRY") ||
          token.ner.equalsIgnoreCase("NUMBER") ||
          token.ner.equalsIgnoreCase("STATE_OR_PROVINCE") ||
          token.ner.equalsIgnoreCase("RELIGION")) {

          var pattern = "\\b" + token.word + "\\b".r
          text = text.replaceAll(pattern, replacementString)
          println(text)
        }
      }
    }

    return text
  }

    def main(args: Array[String]) {
      val spark = SparkSession.builder.master("local").getOrCreate()

      // Create a Spark DataFrame consisting of high and low temperatures
      // by airport code and date.
      val schema = StructType(Array(
        StructField("complaints", StringType, false),
        StructField("Date", DateType, false),
        StructField("TempHighF", IntegerType, false),
        StructField("TempLowF", IntegerType, false)
      ))

      val data = List(
        Row("KRISHNA lives in singapore with account no 454630802346346", Date.valueOf("2021-04-03"), 52, 43),
        Row("OBAMA lives in united states with account no 3563456347543", Date.valueOf("2021-04-02"), 50, 38),
        Row("PETER lives in Malaysia with account no 454630802346346", Date.valueOf("2021-04-01"), 52, 41),
        Row("Bill Gates lives in singapore with mobile number +6594557261", Date.valueOf("2021-04-03"), 64, 45)
      )

      val rdd = spark.sparkContext.makeRDD(data)
      val temps = spark.createDataFrame(rdd, schema)



      val anonymize_udf = udf(anonymize(_:String))

     // import spark.implicits._
      temps.select(anonymize_udf(temps("complaints"))).show(20, truncate = false)

     // val abc= anonymize("KRISHNA lives in singapore with account no 454630802346346",props,pipeline)



    }





  }
