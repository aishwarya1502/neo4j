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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator._
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.annotation.nowarn
import scala.math.Ordering.Implicits._

case class NodeSortMergeJoinPipe(nodeVariables: Set[String], left: Pipe, right: Pipe)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(left) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    if (input.isEmpty)
      return ClosingIterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return ClosingIterator.empty

    // Collect and sort the left input based on the join keys
    val leftRows = input.toList
    val sortedLeftRows = leftRows.sortBy(row => computeKey(row))

    // Collect and sort the right input based on the join keys
    val rightRows = rhsIterator.toList
    val sortedRightRows = rightRows.sortBy(row => computeKey(row))

    // Merge the sorted lists
    val resultIterator = mergeSortedIterators(sortedLeftRows.iterator, sortedRightRows.iterator, state)

    ClosingIterator(resultIterator)
  }

  private val cachedVariables = nodeVariables.toIndexedSeq

  @nowarn("msg=return statement")
  private def computeKey(context: CypherRow): Seq[Long] = {
    val key = new Array[Long](cachedVariables.length)

    for (idx <- cachedVariables.indices) {
      key(idx) = context.getByName(cachedVariables(idx)) match {
        case n: VirtualNodeValue => n.id()
        case IsNoValue()         =>
          // Handle missing values appropriately, e.g., return a special value or throw an exception
          return Seq.empty
        case _ =>
          throw new CypherTypeException("Expected a node value for join key but found a different type")
      }
    }
    key.toSeq
  }

  private def mergeSortedIterators(
    leftIter: Iterator[CypherRow],
    rightIter: Iterator[CypherRow],
    state: QueryState
  ): Iterator[CypherRow] = new Iterator[CypherRow] {
    private var leftRow: Option[CypherRow] = if (leftIter.hasNext) Some(leftIter.next()) else None
    private var rightRow: Option[CypherRow] = if (rightIter.hasNext) Some(rightIter.next()) else None
    private var nextResult: Option[CypherRow] = None

    override def hasNext: Boolean = {
      if (nextResult.isDefined) return true

      while (leftRow.isDefined && rightRow.isDefined) {
        val leftKey = computeKey(leftRow.get)
        val rightKey = computeKey(rightRow.get)

        // Skip rows with empty keys (due to nulls or missing values)
        if (leftKey.isEmpty) {
          leftRow = if (leftIter.hasNext) Some(leftIter.next()) else None
        } else if (rightKey.isEmpty) {
          rightRow = if (rightIter.hasNext) Some(rightIter.next()) else None
        } else {
          val comparison = compareKeys(leftKey, rightKey)
          if (comparison == 0) {
            // Keys match, produce joined row
            val output = leftRow.get.createClone()
            output.mergeWith(rightRow.get, state.query)
            nextResult = Some(output)
            rightRow = if (rightIter.hasNext) Some(rightIter.next()) else None
            return true
          } else if (comparison < 0) {
            leftRow = if (leftIter.hasNext) Some(leftIter.next()) else None
          } else {
            rightRow = if (rightIter.hasNext) Some(rightIter.next()) else None
          }
        }
      }
      false
    }

    override def next(): CypherRow = {
      if (!hasNext) throw new NoSuchElementException("No more elements")
      val result = nextResult.get
      nextResult = None
      result
    }

    private def compareKeys(leftKey: Seq[Long], rightKey: Seq[Long]): Int = {
      for (i <- leftKey.indices) {
        val cmp = java.lang.Long.compare(leftKey(i), rightKey(i))
        if (cmp != 0) return cmp
      }
      0
    }
  }
}
