/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.util.JsonUtils
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.org.apache.http.HttpHost
import org.opensearch.search.builder.SearchSourceBuilder

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * OpenSearchLogStore.
 */
class OpenSearchLogStore(
  sparkConf: SparkConf,
  hadoopConf: Configuration
) extends LogStore with Logging {

  val INDEX_NAME = "deltalog"

  val INDEX_META_FIELD = Set("path", "version")

  val OPENSEARCH_HOST = "spark.opensearch.host"
  val OPENSEARCH_HOST_DEFAULT = "localhost"

  val hostname = sparkConf.get(OPENSEARCH_HOST, OPENSEARCH_HOST_DEFAULT)

  /**
   * Load the given file and return a `Seq` of lines. The line break will be removed from each
   * line. This method will load the entire file into the memory. Call `readAsIterator` if possible
   * as its implementation may be more efficient.
   */
  override def read(path: Path): Seq[String] = {
    val client = new RestHighLevelClient(RestClient
      .builder(new HttpHost(hostname, 9200, "http")))

    try {
      val sourceBuilder = new SearchSourceBuilder
      sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS))
      sourceBuilder.size(1)
      sourceBuilder.from(0)
      sourceBuilder.query(QueryBuilders.termQuery("path", path.toString))

      val searchRequest = new SearchRequest
      searchRequest.indices(INDEX_NAME)
      searchRequest.source(sourceBuilder)

      logInfo(s"===== read $path =====")

      val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

      val hits = searchResponse.getHits.getHits
      if (hits.isEmpty) {
        Seq.empty
      } else {
        val content = hits(0).getSourceAsMap.asScala
          .filterKeys(!INDEX_META_FIELD(_))
        content.map(t => {
          val action = JsonUtils.mapper.writeValueAsString(Map(t._1 -> t._2))
          logInfo(s"===== read action: $action =====")
          action
        }).toSeq
      }
    } catch {
      case ex: Throwable =>
        logError("read", ex)
        Seq.empty
    } finally {
      client.close()
    }
  }

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   */
  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    val client = new RestHighLevelClient(RestClient
      .builder(new HttpHost(hostname, 9200, "http")))

    try {
      val pathName = path.getName
      val version = pathName.substring(pathName.lastIndexOf('/') + 1).split('.')(0)
      val sourceActions = actions.map(_.replaceAll("^.|.$", "")).toList.mkString(",")
      val source = s"""{"path":"$path","version":"$version",$sourceActions}"""

      logInfo(s"===== write $path =====")
      logInfo(source)

      val request = new IndexRequest(INDEX_NAME).source(source, XContentType.JSON)
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
      val response = client.index(request, RequestOptions.DEFAULT)

      logInfo(s"===== write ${response.getResult} ======")
    } finally {
      client.close()
    }
  }

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  override def listFrom(path: Path): Iterator[FileStatus] = {
    val client = new RestHighLevelClient(RestClient
      .builder(new HttpHost(hostname, 9200, "http")))

    try {
      val pathName = path.getName
      val version = pathName.substring(pathName.lastIndexOf('/') + 1).split('.')(0)

      val sourceBuilder = new SearchSourceBuilder
      sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS))
      sourceBuilder.size(10000)
      sourceBuilder.from(0)
      sourceBuilder.query(QueryBuilders.rangeQuery("version").gte(version))
      sourceBuilder.sort("path")

      val searchRequest = new SearchRequest
      searchRequest.indices(INDEX_NAME)
      searchRequest.source(sourceBuilder)

      logInfo(s"===== listFrom $path =====")

      val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

      searchResponse.getHits.getHits
        .map(_.getSourceAsMap.get("path").toString)
        .map(new Path(_))
        .map(new FileStatus(0, false, 0, 0, 0, _))
        .toIterator
    } finally {
      client.close()
    }
  }

  /** Invalidate any caching that the implementation may be using */
  override def invalidateCache(): Unit = {
    // do nothing
  }

  override def isPartialWriteVisible(path: Path): Boolean = false
}
