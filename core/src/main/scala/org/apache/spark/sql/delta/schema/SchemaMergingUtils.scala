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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types._

/**
 * Utils to merge table schema with data schema.
 * This is split from SchemaUtils, because finalSchema is introduced into DeltaMergeInto,
 * and resolving the final schema is now part of.
 */
object SchemaMergingUtils {

  val DELTA_COL_RESOLVER: (String, String) => Boolean =
    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  /**
   * Returns pairs of (full column name path, field) in this schema as a list. For example, a schema
   * like:
   *   <field a>          | - a
   *   <field 1>          | | - 1
   *   <field 2>          | | - 2
   *   <field b>          | - b
   *   <field c>          | - c
   *   <field `foo.bar`>  | | - `foo.bar`
   *   <field 3>          |   | - 3
   *   will return [
   *     ([a], <field a>), ([a, 1], <field 1>), ([a, 2], <field 2>), ([b], <field b>),
   *     ([c], <field c>), ([c, foo.bar], <field foo.bar>), ([c, foo.bar, 3], <field 3>)
   *   ]
   */
  def explode(schema: StructType): Seq[(Seq[String], StructField)] = {
    def recurseIntoComplexTypes(complexType: DataType): Seq[(Seq[String], StructField)] = {
      complexType match {
        case s: StructType => explode(s)
        case a: ArrayType => recurseIntoComplexTypes(a.elementType)
          .map { case (path, field) => (Seq("element") ++ path, field) }
        case m: MapType =>
          recurseIntoComplexTypes(m.keyType)
            .map { case (path, field) => (Seq("key") ++ path, field) } ++
          recurseIntoComplexTypes(m.valueType)
            .map { case (path, field) => (Seq("value") ++ path, field) }
        case _ => Nil
      }
    }

    schema.flatMap {
      case f @ StructField(name, s: StructType, _, _) =>
        Seq((Seq(name), f)) ++
          explode(s).map { case (path, field) => (Seq(name) ++ path, field) }
      case f @ StructField(name, a: ArrayType, _, _) =>
        Seq((Seq(name), f)) ++
          recurseIntoComplexTypes(a).map { case (path, field) => (Seq(name) ++ path, field) }
      case f @ StructField(name, m: MapType, _, _) =>
        Seq((Seq(name), f)) ++
          recurseIntoComplexTypes(m).map { case (path, field) => (Seq(name) ++ path, field) }
      case f => (Seq(f.name), f) :: Nil
    }
  }

  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    explode(schema).map { case (path, _) => path }.map(UnresolvedAttribute.apply(_).name)
  }
}
