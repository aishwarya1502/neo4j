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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.util.attribution.Id

sealed abstract class Argument extends Product {

  def name: String = productPrefix
}

/**
 * A String that is guaranteed to be free of autogenerated names and with backticks in the right places.
 */
case class PrettyString private[plandescription] (prettifiedString: String) {
  override def toString: String = prettifiedString

  def repeat(count: Int): PrettyString = PrettyString(prettifiedString.repeat(count))
  def stripMargin: PrettyString = PrettyString(prettifiedString.stripMargin)
}

object PrettyString {
  implicit val byPrettifiedString: Ordering[PrettyString] = Ordering.by(_.prettifiedString)
}

object Arguments {
  private val VERSION_PATTERN = "(\\d+)\\.{1}(\\d+).*".r

  val CURRENT_VERSION: String =
    parseMajorMinor(parseMajorMinor(org.neo4j.kernel.internal.Version.getNeo4jVersion))

  object Details {

    def apply(details: PrettyString): Details = {
      Details(Seq(details))
    }
  }

  // For calling the apply method of EstimatedRows from Java
  def estimatedRows(effectiveCardinality: Double): EstimatedRows = {
    EstimatedRows(effectiveCardinality, Some(effectiveCardinality))
  }

  /**
   * This will get rendered in the Details column.
   * Each String in info can be of two kinds:
   * 1) If the String contains no newline characters, it is considered a single-line detail.
   *    We will try to render these together (separated by a comma and a space) with other
   *    single-line details on the same line, until a line is full.
   *    If a single-line detail is longer than the maximal line length, it will be split.
   * 2) If the String contains newline characters, it is considered a multi-line detail.
   *    A multi-line detail occupies multiple lines, and will not share a line with other details.
   *    If a line of a multi-line detail is longer than the maximal line length, it will be split.
   */
  case class Details(info: collection.Seq[PrettyString]) extends Argument

  case class Time(value: Long) extends Argument

  case class Rows(value: Long) extends Argument

  case class DbHits(value: Long) extends Argument

  case class Memory(value: Long) extends Argument

  case class GlobalMemory(value: Long) extends Argument

  case class AvailableWorkers(value: Int) extends Argument

  case class Order(order: PrettyString) extends Argument

  case class Distinctness(distinctness: PrettyString) extends Argument

  case class PageCacheHits(value: Long) extends Argument

  case class PageCacheMisses(value: Long) extends Argument

  case class EstimatedRows(effectiveCardinality: Double, cardinality: Option[Double] = None) extends Argument

  case class PipelineInfo(pipelineId: Int, fused: Boolean) extends Argument

  // This is the version of cypher
  case class Version(value: String) extends Argument {

    override def name = "version"
  }

  case class RuntimeVersion(value: String) extends Argument {

    override def name = "runtime-version"
  }

  object RuntimeVersion {
    def currentVersion: RuntimeVersion = RuntimeVersion(CURRENT_VERSION)
  }

  case class Planner(value: String) extends Argument {

    override def name = "planner"
  }

  case class PlannerImpl(value: String) extends Argument {

    override def name = "planner-impl"
  }

  case class PlannerVersion(value: String) extends Argument {

    override def name = "planner-version"
  }

  object PlannerVersion {
    def currentVersion: PlannerVersion = PlannerVersion(CURRENT_VERSION)
  }

  case class Runtime(value: String) extends Argument {

    override def name = "runtime"
  }

  case class RuntimeImpl(value: String) extends Argument {

    override def name = "runtime-impl"
  }

  case class SourceCode(className: String, sourceCode: String) extends Argument {

    override def name: String = "source:" + className
  }

  case class ByteCode(className: String, disassembly: String) extends Argument {

    override def name: String = "bytecode:" + className
  }

  case class BatchSize(size: Int) extends Argument {

    override def name: String = "batch-size"
  }

  case class StringRepresentation(value: String) extends Argument {
    override def name: String = "string-representation"
  }

  case class IdArg(id: Id) extends Argument {
    override def name: String = "Id"
  }

  def parseMajorMinor(version: String): String = {
    version match {
      case VERSION_PATTERN(major, minor) => s"$major.$minor"
      case _                             => version
    }
  }
}
