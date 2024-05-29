/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

import java.util.Collections

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  protected def context: QueryContext

  protected def getOptionsParts(
    options: MapValue,
    schemaType: String,
    indexType: IndexType
  ): (Option[IndexProviderDescriptor], IndexConfig) = {

    if (options.exists { case (k, _) => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig") }) {
      throw new InvalidArgumentsException(
        s"Failed to create $schemaType: Invalid option provided, valid options are `indexProvider` and `indexConfig`."
      )
    }
    val maybeIndexProvider = options.getOption("indexprovider")
    val maybeConfig = options.getOption("indexconfig")

    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, schemaType, indexType))
    val configMap: java.util.Map[String, Object] =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType, indexProvider)).getOrElse(Collections.emptyMap())
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)

    (indexProvider, indexConfig)
  }

  protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object]

  private def assertValidIndexProvider(
    indexProvider: AnyValue,
    schemaType: String,
    indexType: IndexType
  ): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      context.validateIndexProvider(schemaType, indexProviderValue.stringValue(), indexType)
    case _ =>
      throw new InvalidArgumentsException(
        s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value."
      )
  }

  protected def checkForPointConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p: String, _) =>
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains spatial config settings options.
           |To create point index, please use 'CREATE POINT INDEX ...'.""".stripMargin
      )
    }

  protected def checkForFulltextConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p, _) =>
        p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(
          FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName
        )
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains fulltext config options.
           |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin
      )
    }

  protected def checkForVectorConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p, _) =>
        p.equalsIgnoreCase(VECTOR_DIMENSIONS.getSettingName) || p.equalsIgnoreCase(
          VECTOR_SIMILARITY_FUNCTION.getSettingName
        )
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains vector config options.
           |To create vector index, please use 'CREATE VECTOR INDEX ...'.""".stripMargin
      )
    }

  protected def assertEmptyConfig(
    config: AnyValue,
    schemaType: String,
    indexType: String
  ): java.util.Map[String, Object] = {
    // no available config settings, throw nice error when existing config settings for other index types
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(pp, itemsMap, schemaType)
        checkForPointConfigValues(pp, itemsMap, schemaType)
        checkForVectorConfigValues(pp, itemsMap, schemaType)

        if (!itemsMap.isEmpty) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create $schemaType with specified index config '${pp.value()}': $indexType indexes have no valid config values.""".stripMargin
          )
        }

        Collections.emptyMap()
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(
          s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map."
        )
    }
  }
}

case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])

case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateWithNoOptions()