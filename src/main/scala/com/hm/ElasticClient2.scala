package com.hm

import java.io.File
import java.net.{InetAddress, NetworkInterface}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.slf4j.LoggerFactory
import spray.json._
import com.typesafe.config._
import scala.collection.JavaConversions._
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.{AggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.transport.client.PreBuiltTransportClient
/**
  * Created by hari on 21/3/17.
  */
object ElasticClient2 {

  val IP_ADDRESS: String = NetworkInterface.getNetworkInterfaces.toIterator.flatMap(_.getInetAddresses.toSeq).find(address =>
    address.getHostAddress.contains(".") && !address.isLoopbackAddress
  ).getOrElse(InetAddress.getLocalHost).getHostAddress
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val BULK_REQUEST_BATCH_SIZE = 10
  private val bulkRequestCounter = new AtomicInteger()
  private val configFile = new File("server/src/main/resources/application.conf")

  val config: Config = ConfigFactory.parseFile(configFile).withValue("ip", ConfigValueFactory.fromAnyRef(IP_ADDRESS))
  private val hostName ="localhost"

  private val clusterName ="local_es"

  private val transportPort = 9300

  private val settings = Settings.builder().put("cluster.name",clusterName).build()

  private var transportClient: Client = null

  private val bulkRequest: BulkRequestBuilder = getTransportClient.prepareBulk()

  /**
    * Initialise the transport client
    *
    */

  private def init(): Unit = try {
    transportClient = new PreBuiltTransportClient(settings).addTransportAddress(
      new InetSocketTransportAddress(InetAddress.getByName(hostName), transportPort)
    )
    LOG.info("New Elastic Search Client Initiated")
  } catch {
    case e: Exception => LOG.error("Exception During Elastic Search Initialization", e)
  }

  /**
    * Gets the transportclient object
    * @return
    */

  def getTransportClient: Client = {
    if (transportClient == null) init()
    transportClient
  }

  def createIndex(index: String): Boolean = {
    getTransportClient.admin().indices().prepareExists(index).execute().actionGet().isExists
  }

  /**
    * Prepares the hashmap of field name and values
    *
    * @param elements map of fieldname and values
    * @return key value pair of fieldname and elements
    */

  def prepareJsonDocument(elements: Map[String, Any]): util.HashMap[String, Object] = {
    val hashMap = new util.HashMap[String, Object]()
    try {
      elements.foreach(kv => {
        hashMap.put(kv._1, kv._2.asInstanceOf[Object])
      })
    } catch {
      case e: Exception => LOG.error(e.getLocalizedMessage + " while creating JsonDocument", e)
    }
    hashMap
  }

  /**
    * Insert Bulk data to the given index and tag name.
    *
    * @param indexName
    * @param tagName
    * @param elements
    * @param id
    */
  def bulkUpsert(indexName: String, tagName: String, elements: Map[String, Any], id: Long): Unit = {

    val content = prepareJsonDocument(elements)

    bulkRequest.add(getTransportClient.prepareUpdate(indexName, tagName, id.toString).setDoc(content).setUpsert(content))

    if (bulkRequestCounter.incrementAndGet() % BULK_REQUEST_BATCH_SIZE == 0) updateBulkRequest()

  }

  /**
    * Bulk request
    *
    */
  def updateBulkRequest(): Unit = try {
    LOG.info("Elastic Search Updating Bulk Request")
    bulkRequest.get()
  } catch {
    case e: Exception => LOG.error("Elastic Search Exception While Updating Bulk Request", e)
  }

  /**
    * Build the query for given set of fields
    *
    * @param query
    * @return
    */

  def buildQueryBuilder(query: Map[String, Map[String, Any]]): QueryBuilder = {
    val boolQuery = QueryBuilders.boolQuery()
    query.foreach(querySubset => {
      querySubset._1 match {
        case "range" => querySubset._2.foreach(elements => {

          boolQuery.must(QueryBuilders.rangeQuery(elements._1)
            .from(elements._2.asInstanceOf[(Long, Long)]._1).to(elements._2.asInstanceOf[(Long, Long)]._2))

        })
        case "term" => querySubset._2.foreach(elements => {

          boolQuery.must(QueryBuilders.termQuery(elements._1, elements._2))
        })
      }
    })
    if(query.isEmpty) QueryBuilders.matchAllQuery() else boolQuery
  }

  /**
    * Build the aggregation for given fields
    *
    * @param aggregationArray
    * @return
    */

  def buildAggregateQuery(
                           aggregationArray: Array[(String, String)]
                         ): AggregationBuilder = {
    try {
      val a = aggregationArray.map(i => AggregationBuilders.terms(i._1).field(i._2)).reduceRight((i, j) => i.subAggregation(j))


      a.size(100)
      a
    }
    catch {
      case e: Exception => "send the query with the aggregation format"
        AggregationBuilders.missing("")
    }
  }


  /**
    * Build SearchRequestBuilder for given query and aggrgation object
    *
    * @param indexName
    * @param tagName
    * @param query
    * @param aggregationArray
    * @return
    */
  def buildSearchQuery(
                        indexName: String,
                        tagName: String,
                        query: Map[String, Map[String, Any]],
                        aggregationArray: Array[(String, String)]
                      ): SearchRequestBuilder = getTransportClient.prepareSearch(indexName).setTypes(tagName)
    .setQuery(buildQueryBuilder(query.asInstanceOf[Map[String, Map[String, Any]]])).addAggregation(buildAggregateQuery(aggregationArray))


  /**
    * Gets field and data type for given indexname and tagname
    *
    * @param indexName
    * @param tagName
    * @return
    */

  def getDataTypeMapping(indexName: String, tagName: String): Map[String, String] = try {
    getTransportClient.admin().cluster().prepareState().setIndices(indexName).get()
      .getState.getMetaData.index(indexName).mapping(tagName).getSourceAsMap.toMap
      .get("properties") match {
      case Some(obj) => obj.asInstanceOf[util.Map[String, Object]].toMap.map(i =>
        i._1 -> i._2.asInstanceOf[util.Map[String, Object]].get("type").asInstanceOf[String]
      )
      case None => Map()
    }
  } catch {
    case e: Exception => LOG.error("Error While getDataTypeMapping for " + indexName + ":" + tagName, e)
      Map()
  }


  /**
    * Iterate through all the documents in elasticsearch
    * @param indexName
    * @param tagName
    * @param keyFieldName
    * @param requiredCols
    * @param function
    * @param queryMap
    * @param batchSize
    */
  def iterateAll(indexName: String,
                 tagName: String,
                 keyFieldName: String,
                 requiredCols: Array[String],
                 function: Map[String, Any] => Unit,

                 queryMap: Map[String, Map[String, Any]],
                 batchSize:Long=10000L): Unit = {


    val minMax = getMinMaxLong(indexName, tagName, keyFieldName)
    var i = minMax._1
    val incSize = batchSize

    var tmpQueryMap = queryMap

    tmpQueryMap = tmpQueryMap.get("range") match {
      case Some(map) => tmpQueryMap + ("range" -> (map + (keyFieldName -> (i, i + incSize))))
      case None => tmpQueryMap + ("range" -> Map(keyFieldName -> (i, i + incSize)))
    }


    while (i < minMax._2) {

      val srb: SearchRequestBuilder = getTransportClient.prepareSearch().setIndices(indexName).setTypes(tagName)
      srb.setQuery(ElasticClient2.buildQueryBuilder(tmpQueryMap))

      srb.addSort(keyFieldName, SortOrder.ASC)
      srb.setSize(incSize.toInt)
      println("Before "+i+"<"+minMax)
      val get=srb.get()
      println("After "+i+"<"+minMax)
      val hits = get.getHits.hits()
      hits.foreach(dataRec => try {
        val tmp = dataRec.sourceAsMap().map(e => (e._1, e._2.asInstanceOf[Any])).filter(e => requiredCols.contains(e._1)).toMap
        function(tmp)
      }catch {
        case e:Exception=>e.printStackTrace()
      })
//      ConsoleUtils.consoleStatusWriter(i + " :: " + hits.length)
      println(i + " :: " + hits.length)
      i = i + incSize

      tmpQueryMap = queryMap

      tmpQueryMap = tmpQueryMap.get("range") match {
        case Some(map) => tmpQueryMap + ("range" -> (map + (keyFieldName -> (i, i + incSize))))
        case None => tmpQueryMap + ("range" -> Map(keyFieldName -> (i, i + incSize)))
      }
    }
    println("exit at while")
  }


  def iterateAll2(indexName: String,
                 tagName: String,
                 keyFieldName: String,
                 requiredCols: Array[String],
                 function: Map[String, Any] => Unit,

                 queryMap: Map[String, Map[String, Any]],
                 batchSize:Int=1000): Unit = {

    val srbt: SearchRequestBuilder = getTransportClient.prepareSearch().setIndices(indexName).setTypes(tagName)
    var i = 0
    val totalRequests = srbt.setSize(1).setQuery(buildQueryBuilder(queryMap)).get().getHits.getTotalHits

    println(totalRequests)

    while (i < totalRequests) {

      val srb: SearchRequestBuilder = getTransportClient.prepareSearch().setIndices(indexName).setTypes(tagName)
      srb.setQuery(ElasticClient2.buildQueryBuilder(queryMap)).setFrom(i).setSize(batchSize)

//      println("Before "+i+"<"+totalRequests)
      val get=srb.get()
//      println("After "+i+"<"+totalRequests)
      val hits = get.getHits.hits()
      hits.foreach(dataRec => try {
        val tmp = dataRec.sourceAsMap().map(e => (e._1, e._2.asInstanceOf[Any])).filter(e => requiredCols.contains(e._1)).toMap
//        println(tmp)
        function(tmp)
      }catch {
        case e:Exception=>e.printStackTrace()
      })

      i = i+batchSize
ConsoleUtils.consoleStatusWriter(i + " :: " + hits.length+ " -- "+"Before "+i+"<"+totalRequests)
//      println(i + " :: " + hits.length)

    }
    println("exit at while")
  }


  /**
    * Get the jsobject of aggregation which contains min and max
    *
    * @param indexName
    * @param tagName
    * @param colName
    * @return
    */
  def getMinMaxLong(indexName: String, tagName: String, colName: String): (Long,Long)= {
    val srb: SearchRequestBuilder = getTransportClient.prepareSearch().setIndices(indexName).setTypes(tagName)
    srb.setQuery(QueryBuilders.matchAllQuery())
    srb.addAggregation(AggregationBuilders.extendedStats("minmax").field(colName))
    srb.setSize(1)
    val sr: SearchResponse = srb.get
    val jsObject =sr.toString.parseJson.asJsObject.getFields("aggregations").head.asJsObject.getFields("minmax").head.asJsObject
    val min=jsObject.getFields("min").head.asInstanceOf[JsNumber].value.toLong
    val max=jsObject.getFields("max").head.asInstanceOf[JsNumber].value.toLong
    (min,max)
  }
  def getMinMaxDouble(indexName: String, tagName: String, colName: String): (Double,Double)= {
    val srb: SearchRequestBuilder = getTransportClient.prepareSearch().setIndices(indexName).setTypes(tagName)
    srb.setQuery(QueryBuilders.matchAllQuery())
    srb.addAggregation(AggregationBuilders.extendedStats("minmax").field(colName))
    srb.setSize(1)
    val sr: SearchResponse = srb.get
    val jsObject =sr.toString.parseJson.asJsObject.getFields("aggregations").head.asJsObject.getFields("minmax").head.asJsObject
    val min=jsObject.getFields("min").head.asInstanceOf[JsNumber].value.toDouble
    val max=jsObject.getFields("max").head.asInstanceOf[JsNumber].value.toDouble
    (min,max)
  }

}
