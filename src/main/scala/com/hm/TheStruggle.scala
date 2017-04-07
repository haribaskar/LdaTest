package com.hm

/**
  * Created by swathi on 3/4/17.
  */
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
object TheStruggle {


  var i=0
  val id=new AtomicInteger()
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("LatentDirichletAllocationExample").setMaster("local")
    val sc = new SparkContext(conf)
    // Load documents from text files, 1 document per file
    val s=sc.wholeTextFiles("data.txt").map(_._1)
    val corpus: RDD[String] = sc.wholeTextFiles("data.txt").map(_._2)
      println("c::"+s.first())

    // Split each document into a sequence of terms (words)
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
    tokenized.foreach(println)

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    //   vocabArray: Chosen vocab (removing common terms)
    println("********")
    tokenized.foreach(println)
    val numStopwords = 300
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    //   vocab: Map term -> term index
    println("vocabarray" + vocabArray.mkString(","))
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 10
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)
    //  val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")


      i=i+1
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
        var kvMap= Map[String,Any]()
          kvMap +=("Topic"->i)
          kvMap +=(vocabArray(term.toInt)->weight)
        //  insert("topic","test","_id",kvMap)
      }
      println()
    }

    val testDocument = documents.first()

    println("^^^^^^^^^^&&&&&&&&&&&&&&&&^^^^^^^^^^^ " + ldaModel.getClass.getCanonicalName)

    val localLDAModel = ldaModel.asInstanceOf[DistributedLDAModel].toLocal

    val doc = (testDocument._1, localLDAModel.topicDistribution(testDocument._2))


    println(doc._2.toArray.zipWithIndex.maxBy(_._1))
    println(doc._2.toArray.zipWithIndex.minBy(_._1))

//    val value = minMax(topicDistribution.toArray)

//    println("::" + value)
  }
      def insert(indexName:String,tagName:String,keyFieldName:String,map:Map[String,Any]): Unit ={
        //ElasticClient2.createIndex(indexName)
        /*var idCount: Long = try {
          ElasticClient2.getMinMaxLong(indexName, tagName, keyFieldName)._2
        } catch {
          case e: Exception => println("Error while getting idcount" + e)
            0
        }*/

         //println("idcount"+idCount)
        println("map"+map)
        ElasticClient2.bulkUpsert(indexName, tagName, map, id.getAndIncrement())
        println("elastic search bulk upsert")
      }
}
