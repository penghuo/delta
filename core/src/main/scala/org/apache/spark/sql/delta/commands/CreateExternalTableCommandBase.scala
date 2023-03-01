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

package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaSQLConf, DeltaSourceUtils}
import org.apache.spark.sql.delta.util._
import org.apache.spark.sql.delta.{
  DeltaErrors,
  DeltaLog,
  DeltaOperations,
  OptimisticTransaction
}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.{
  ParquetFileFormat,
  ParquetToSparkSchemaConverter
}
import org.apache.spark.sql.execution.streaming.{
  FileStreamSink,
  MetadataLogFileIndex
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable
import java.util.Locale
import scala.collection.JavaConverters.asScalaIteratorConverter

abstract class CreateExternalTableCommandBase(
  table: CatalogTable,
  targetDir: Path
) extends RunnableCommand
  with DeltaCommand {

  lazy val partitionColNames: Seq[String] = Nil

  lazy val partitionFields: Seq[StructField] = Nil

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  override def run(spark: SparkSession): Seq[Row] = {
    val convertProperties =
      ConvertTarget(table, targetDir, Map.empty[String, String])

    val deltaLog = DeltaLog.forTable(spark, targetDir)
    val txn = deltaLog.startTransaction()

    performConvert(spark, txn, convertProperties)
  }

  /**
   * Override this method since parquet paths are valid for Convert
   *
   * @param tableIdent
   * the provided table or path
   * @return
   * Whether or not the ident provided can refer to a table by path
   */
  override def isPathIdentifier(tableIdent: TableIdentifier): Boolean = {
    val provider = tableIdent.database.getOrElse("")
    // If db doesnt exist or db is called delta/tahoe then check if path exists
    (DeltaSourceUtils.isDeltaDataSourceName(
      provider
    ) || provider == "parquet") &&
      new Path(tableIdent.table).isAbsolute
  }

  /**
   * Generate a file manifest for the table with the given base path
   * `qualifiedDir`.
   */
  protected def getFileManifest(
    spark: SparkSession,
    qualifiedDir: String,
    serializableConf: SerializableConfiguration
  ): FileManifest = {
    if (
      conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_METADATA_LOG) &&
        FileStreamSink.hasMetadata(
          Seq(qualifiedDir),
          serializableConf.value,
          conf
        )
    ) {
      new MetadataLogFileManifest(spark, qualifiedDir)
    } else new ManualListingFileManifest(spark, qualifiedDir, serializableConf)
  }

  /**
   * Given the file manifest, create corresponding AddFile actions for the
   * entire list of files.
   */
  protected def createDeltaActions(
    spark: SparkSession,
    manifest: FileManifest,
    txn: OptimisticTransaction,
    fs: FileSystem
  ): Iterator[AddFile] = {
    val statsBatchSize =
      conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_STATS_COLLECTION)
    manifest.getFiles.grouped(statsBatchSize).flatMap { batch =>
      val adds = batch.map(createAddFile(_, txn.deltaLog.dataPath, fs, conf))
      adds.toIterator
    }
  }

  /** Infers the schema from a batch of parquet files. */
  protected def getSchemaForBatch(
    spark: SparkSession,
    batch: Seq[SerializableFileStatus],
    serializedConf: SerializableConfiguration
  ): StructType = {
    mergeSchemasInParallel(spark, batch.map(_.toFileStatus), serializedConf)
      .getOrElse(
        throw new RuntimeException(
          "Failed to infer schema from the given list of files."
        )
      )
  }

  /**
   * Converts the given table to a Delta table. First gets the file manifest
   * for the table. Then in the first pass, it infers the schema of the table.
   * Then in the second pass, it generates the relevant Actions for Delta's
   * transaction log, namely the AddFile actions for each file in the manifest.
   * Once a commit is made, updates an external catalog, e.g. Hive MetaStore,
   * if this table was referenced through a table in a catalog.
   */
  private def performConvert(
    spark: SparkSession,
    txn: OptimisticTransaction,
    convertProperties: ConvertTarget
  ): Seq[Row] =
    recordDeltaOperation(txn.deltaLog, "delta.convert") {

      txn.deltaLog.ensureLogDirectoryExist()
      val targetPath = convertProperties.targetDir
      val sessionHadoopConf = spark.sessionState.newHadoopConf()
      val fs = targetPath.getFileSystem(sessionHadoopConf)
      val qualifiedPath = fs.makeQualified(targetPath)
      val qualifiedDir = qualifiedPath.toString
      if (!fs.exists(qualifiedPath)) {
        throw DeltaErrors.pathNotExistsException(qualifiedDir)
      }
      val serializableConfiguration =
        new SerializableConfiguration(sessionHadoopConf)
      val manifest =
        getFileManifest(spark, qualifiedDir, serializableConfiguration)

      try {
        val initialList = manifest.getFiles
        if (!initialList.hasNext) {
          throw DeltaErrors.emptyDirectoryException(qualifiedDir)
        }

        val schemaBatchSize =
          spark.sessionState.conf.getConf(
            DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE
          )
        var dataSchema: StructType = StructType(Seq())
        var numFiles = 0L
        recordDeltaOperation(txn.deltaLog, "delta.convert.schemaInference") {
          initialList.grouped(schemaBatchSize).foreach { batch =>
            numFiles += batch.size
            // Obtain a union schema from all files.
            // Here we explicitly mark the inferred schema nullable. This also means we don't
            // currently support specifying non-nullable columns after the table conversion.
            val batchSchema = getSchemaForBatch(
              spark,
              batch,
              serializableConfiguration
            ).asNullable
            dataSchema = SchemaUtils.mergeSchemas(dataSchema, batchSchema)
          }
        }

        val schema = constructTableSchema(spark, dataSchema, partitionFields)
        val metadata = Metadata(
          schemaString = schema.json,
          partitionColumns = partitionColNames,
          configuration = convertProperties.properties
        )
        txn.updateMetadataForNewTable(metadata)

        val addFilesIter = createDeltaActions(spark, manifest, txn, fs)
        val metrics = Map[String, String](
          "numConvertedFiles" -> numFiles.toString
        )

        commitLarge(
          spark,
          txn,
          Iterator.single(txn.protocol) ++ addFilesIter,
          getOperation(numFiles, convertProperties),
          getContext,
          metrics
        )
      } finally {
        manifest.close()
      }

      Seq.empty[Row]
    }

  protected def createAddFile(
    file: SerializableFileStatus,
    basePath: Path,
    fs: FileSystem,
    conf: SQLConf
  ): AddFile = {
    val path = file.getPath
    val pathStr = file.getPath.toUri.toString
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(
        timestampPartitionPattern,
        java.util.TimeZone.getDefault
      )
    val resolver = conf.resolver
    val dir = if (file.isDir) file.getPath else file.getPath.getParent
    val (partitionOpt, _) = PartitionUtils.parsePartition(
      dir,
      typeInference = false,
      basePaths = Set(basePath),
      userSpecifiedDataTypes = Map.empty,
      validatePartitionColumns = false,
      java.util.TimeZone.getDefault,
      dateFormatter,
      timestampFormatter
    )

    val partition = partitionOpt
      .map { partValues =>
        if (partitionColNames.size != partValues.columnNames.size) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr,
            partValues.columnNames,
            partitionColNames
          )
        }

        val tz = Option(conf.sessionLocalTimeZone)
        // Check if the partition value can be casted to the provided type
        partValues.literals.zip(partitionFields).foreach {
          case (literal, field) =>
            if (
              literal.eval() != null && Cast(literal, field.dataType, tz)
                .eval() == null
            ) {
              val partitionValue = Cast(literal, StringType, tz).eval()
              val partitionValueStr =
                Option(partitionValue).map(_.toString).orNull
              throw DeltaErrors
                .castPartitionValueException(partitionValueStr, field.dataType)
            }
        }

        val values = partValues.literals
          .map(l => Cast(l, StringType, tz).eval())
          .map(Option(_).map(_.toString).orNull)

        partitionColNames.zip(partValues.columnNames).foreach {
          case (expected, parsed) =>
            if (!resolver(expected, parsed)) {
              throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(
                pathStr,
                parsed,
                expected
              )
            }
        }
        partitionColNames.zip(values).toMap
      }
      .getOrElse {
        if (partitionColNames.nonEmpty) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr,
            Seq.empty,
            partitionColNames
          )
        }
        Map[String, String]()
      }

    val pathStrForAddFile = if (true) {
      val relativePath =
        DeltaFileOperations.tryRelativizePath(fs, basePath, path)
      assert(
        !relativePath.isAbsolute,
        s"Fail to relativize path $path against base path $basePath."
      )
      relativePath.toUri.toString
    } else {
      path.toUri.toString
    }

    AddFile(
      pathStrForAddFile,
      partition,
      file.length,
      file.modificationTime,
      dataChange = true
    )
  }

  /**
   * Construct a table schema by merging data schema and partition schema. We
   * follow the merge logic in
   * [[org.apache.spark.sql.execution.datasources.HadoopFsRelation]]:
   *
   * When data and partition schemas have overlapping columns, the output
   * schema respects the order of the data schema for the overlapping columns,
   * and it respects the data types of the partition schema.
   */
  protected def constructTableSchema(
    spark: SparkSession,
    dataSchema: StructType,
    partitionFields: Seq[StructField]
  ): StructType = {

    def getColName(f: StructField): String = {
      if (spark.sessionState.conf.caseSensitiveAnalysis) {
        f.name
      } else {
        f.name.toLowerCase(Locale.ROOT)
      }
    }

    val overlappedPartCols = collection.mutable.Map.empty[String, StructField]
    partitionFields.foreach { partitionField =>
      if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
        overlappedPartCols += getColName(partitionField) -> partitionField
      }
    }

    StructType(
      dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
        partitionFields.filterNot(f =>
          overlappedPartCols.contains(getColName(f))
        )
    )
  }

  protected def getContext: Map[String, String] = {
    Map.empty
  }

  /** Get the operation to store in the commit message. */
  protected def getOperation(
    numFilesConverted: Long,
    convertProperties: ConvertTarget
  ): DeltaOperations.Operation = {
    DeltaOperations.Convert(
      numFilesConverted,
      partitionColNames,
      collectStats = false,
      Option(convertProperties.catalogTable.identifier.toString())
    )
  }

  /**
   * This method is forked from [[ParquetFileFormat]]. The only change here is
   * that we use our SchemaUtils.mergeSchemas() instead of StructType.merge(),
   * where we allow upcast between ByteType, ShortType and IntegerType.
   *
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *   1. For a single Parquet part-file, in most cases the footer only resides
   *      in the last block of that file. Thus we only need to retrieve the
   *      location of the last block. However, Hadoop `FileSystem` only
   *      provides API to retrieve locations of all blocks, which can be
   *      potentially expensive.
   *
   * 2. This optimization is mainly useful for S3, where file metadata
   * operations can be pretty slow. And basically locality is not available
   * when using S3 (you can't run computation on S3 nodes).
   */
  protected def mergeSchemasInParallel(
    sparkSession: SparkSession,
    filesToTouch: Seq[FileStatus],
    serializedConf: SerializableConfiguration
  ): Option[StructType] = {
    val assumeBinaryIsString =
      sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp =
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo =
    filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(
      Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism
    )

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
      sparkSession.sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(
              length,
              false,
              0,
              0,
              0,
              0,
              null,
              null,
              null,
              new Path(path)
            )
          }.toSeq

          // Reads footers in multi-threaded manner within each task
          val footers =
            DeltaFileOperations.readParquetFootersInParallel(
              serializedConf.value,
              fakeFileStatuses,
              ignoreCorruptFiles
            )

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val converter = new ParquetToSparkSchemaConverter(
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp
          )
          if (footers.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema =
              ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
            footers.tail.foreach { footer =>
              val schema =
                ParquetFileFormat.readSchemaFromFooter(footer, converter)
              try {
                mergedSchema = SchemaUtils.mergeSchemas(mergedSchema, schema)
              } catch {
                case cause: AnalysisException =>
                  throw new SparkException(
                    s"Failed to merge schema of file ${footer.getFile}:\n${schema.treeString}",
                    cause
                  )
              }
            }
            Iterator.single(mergedSchema)
          }
        }
        .collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        finalSchema = SchemaUtils.mergeSchemas(finalSchema, schema)
      }
      Some(finalSchema)
    }
  }

  protected case class ConvertTarget(
    catalogTable: CatalogTable,
    targetDir: Path,
    properties: Map[String, String]
  )

  private def isHiveStyleParquetTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.provider.contains("hive") && catalogTable.storage.serde
      .contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
  }

  /** An interface for providing an iterator of files for a table. */
  protected trait FileManifest extends Closeable {

    /** The base path of a table. Should be a qualified, normalized path. */
    val basePath: String

    /** Return the active files for a table */
    def getFiles: Iterator[SerializableFileStatus]
  }

  /** A file manifest generated through recursively listing a base path. */
  class ManualListingFileManifest(
    spark: SparkSession,
    override val basePath: String,
    serializableConf: SerializableConfiguration
  ) extends FileManifest {

    protected def doList(): Dataset[SerializableFileStatus] = {
      val conf = spark.sparkContext.broadcast(serializableConf)
      DeltaFileOperations
        .recursiveListDirs(spark, Seq(basePath), conf)
        .where("!isDir")
    }

    private lazy val list: Dataset[SerializableFileStatus] = {
      val ds = doList()
      ds.cache()
      ds
    }

    override def getFiles: Iterator[SerializableFileStatus] =
      list.toLocalIterator().asScala

    override def close(): Unit = list.unpersist()
  }

  /** A file manifest generated from pre-existing parquet MetadataLog. */
  protected class MetadataLogFileManifest(
    spark: SparkSession,
    override val basePath: String
  ) extends FileManifest {
    val index =
      new MetadataLogFileIndex(spark, new Path(basePath), Map.empty, None)

    override def getFiles: Iterator[SerializableFileStatus] =
      index.allFiles.toIterator
        .map { fs => SerializableFileStatus.fromStatus(fs) }

    override def close(): Unit = {}
  }
}

case class CreateExternalTableCommand(
  table: CatalogTable,
  targetDir: Path
) extends CreateExternalTableCommandBase(
  table,
  targetDir
) {
  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
