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
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

case class ValueSortMergeJoinPipe(
  lhsExpression: Expression,
  rhsExpression: Expression,
  left: Pipe,
  right: Pipe
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(left) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    if (input.isEmpty)
      return ClosingIterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return ClosingIterator.empty

    // Convert input iterators to lists for sorting
    val leftRows = input.toList
    val rightRows = rhsIterator.toList

    // Evaluate join keys and pair them with their rows
    val leftKeyedRows = leftRows.map(row => (lhsExpression(row, state), row))
    val rightKeyedRows = rightRows.map(row => (rhsExpression(row, state), row))

    // Filter out rows with null or NO_VALUE join keys and ensure keys are of type Value
    val filteredLeftKeyedRows = leftKeyedRows.collect {
      case (key: Value, row) if key != Values.NO_VALUE => (key, row)
    }
    val filteredRightKeyedRows = rightKeyedRows.collect {
      case (key: Value, row) if key != Values.NO_VALUE => (key, row)
    }

    if (filteredLeftKeyedRows.isEmpty || filteredRightKeyedRows.isEmpty)
      return ClosingIterator.empty

    // Sort both sides based on join keys
    val sortedLeft = filteredLeftKeyedRows.sortBy(_._1)(ValueComparator)
    val sortedRight = filteredRightKeyedRows.sortBy(_._1)(ValueComparator)

    // Perform the merge join
    val resultIterator = new SortMergeJoinIterator(sortedLeft.iterator, sortedRight.iterator, state)

    ClosingIterator(resultIterator)
  }

  // Comparator for Value types
  private object ValueComparator extends Ordering[Value] {
    def compare(x: Value, y: Value): Int = Values.COMPARATOR.compare(x, y)
  }

  // Iterator that performs the sort-merge join
  private class SortMergeJoinIterator(
    leftIter: Iterator[(Value, CypherRow)],
    rightIter: Iterator[(Value, CypherRow)],
    state: QueryState
  ) extends Iterator[CypherRow] {

    private var leftRow: Option[(Value, CypherRow)] = None
    private var rightRow: Option[(Value, CypherRow)] = None
    private var rightBuffer: List[CypherRow] = Nil
    private var nextRow: Option[CypherRow] = None

    advanceLeft()
    advanceRight()

    override def hasNext: Boolean = {
      if (nextRow.isDefined) return true

      while (leftRow.isDefined && (rightRow.isDefined || rightBuffer.nonEmpty)) {
        if (rightBuffer.isEmpty) {
          val comparison = ValueComparator.compare(leftRow.get._1, rightRow.get._1)
          if (comparison < 0) {
            advanceLeft()
          } else if (comparison > 0) {
            advanceRight()
          } else {
            // Matching keys, collect all matching right rows
            rightBuffer = collectMatchingRightRows(leftRow.get._1)
            if (rightBuffer.nonEmpty) {
              nextRow = Some(joinRows(leftRow.get._2, rightBuffer.head))
              rightBuffer = rightBuffer.tail
              return true
            } else {
              advanceLeft()
            }
          }
        } else {
          // We have buffered right rows with matching keys
          nextRow = Some(joinRows(leftRow.get._2, rightBuffer.head))
          rightBuffer = rightBuffer.tail
          if (rightBuffer.isEmpty) advanceLeft()
          return true
        }
      }
      false
    }

    override def next(): CypherRow = {
      if (!hasNext) throw new NoSuchElementException
      val result = nextRow.get
      nextRow = None
      result
    }

    private def advanceLeft(): Unit = {
      if (leftIter.hasNext) {
        leftRow = Some(leftIter.next())
      } else {
        leftRow = None
      }
    }

    private def advanceRight(): Unit = {
      if (rightIter.hasNext) {
        rightRow = Some(rightIter.next())
      } else {
        rightRow = None
      }
    }

    private def collectMatchingRightRows(key: Value): List[CypherRow] = {
      val buffer = scala.collection.mutable.ListBuffer[CypherRow]()
      while (rightRow.isDefined && ValueComparator.compare(rightRow.get._1, key) == 0) {
        buffer += rightRow.get._2
        advanceRight()
      }
      buffer.toList
    }

    private def joinRows(leftCtx: CypherRow, rightCtx: CypherRow): CypherRow = {
      val outputRow = leftCtx.createClone()
      outputRow.mergeWith(rightCtx, state.query)
      outputRow
    }
  }
}
