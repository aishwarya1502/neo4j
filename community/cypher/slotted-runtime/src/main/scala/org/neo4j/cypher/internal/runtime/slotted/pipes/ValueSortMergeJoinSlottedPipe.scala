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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapper
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMappers
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

case class ValueSortMergeJoinSlottedPipe(
  leftSide: Expression,
  rightSide: Expression,
  left: Pipe,
  right: Pipe,
  slots: SlotConfiguration,
  rhsSlotMappings: SlotMappings
)(val id: Id = Id.INVALID_ID)
    extends Pipe {

  private val rhsMappers: Array[SlotMapper] = SlotMappers(rhsSlotMappings)

  override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val lhsInput = left.createResults(state).map(_.asInstanceOf[SlottedRow]).toList
    val rhsInput = right.createResults(state).map(_.asInstanceOf[SlottedRow]).toList

    val lhsKeyedRows = lhsInput.flatMap { row =>
      val key = computeKey(row, leftSide, state)
      key.map(k => (k, row))
    }

    val rhsKeyedRows = rhsInput.flatMap { row =>
      val key = computeKey(row, rightSide, state)
      key.map(k => (k, row))
    }

    val sortedLeft = lhsKeyedRows.sortBy(_._1)(ValueComparator)
    val sortedRight = rhsKeyedRows.sortBy(_._1)(ValueComparator)

    // Use a ClosingIterator here
    val resultIterator = new ValueSortMergeJoinIterator(
      sortedLeft.iterator,
      sortedRight.iterator,
      slots,
      rhsMappers,
      state
    )

    ClosingIterator(resultIterator)
  }

  private def computeKey(
    row: SlottedRow,
    expression: Expression,
    state: QueryState
  ): Option[Value] = {
    val value = expression(row, state)
    value match {
      case v: Value if v != Values.NO_VALUE => Some(v)
      case _                                => None
    }
  }

  private object ValueComparator extends Ordering[Value] {
    def compare(x: Value, y: Value): Int = Values.COMPARATOR.compare(x, y)
  }

  private class ValueSortMergeJoinIterator(
    leftIter: Iterator[(Value, SlottedRow)],
    rightIter: Iterator[(Value, SlottedRow)],
    slots: SlotConfiguration,
    rhsMappers: Array[SlotMapper],
    state: QueryState
  ) extends Iterator[CypherRow] {

    private var leftRowOpt: Option[(Value, SlottedRow)] = None
    private var rightRowOpt: Option[(Value, SlottedRow)] = None
    private var rightBuffer: List[SlottedRow] = Nil
    private var currentLeftKey: Value = _
    private var nextOutput: Option[CypherRow] = None

    advanceLeft()
    advanceRight()

    override def hasNext: Boolean = {
      if (nextOutput.isDefined) return true

      while (leftRowOpt.isDefined && (rightRowOpt.isDefined || rightBuffer.nonEmpty)) {
        if (rightBuffer.isEmpty) {
          val (leftKey, leftRow) = leftRowOpt.get
          val (rightKey, rightRow) = rightRowOpt.get

          val cmp = ValueComparator.compare(leftKey, rightKey)
          if (cmp < 0) {
            advanceLeft()
          } else if (cmp > 0) {
            advanceRight()
          } else {
            // Matching keys
            currentLeftKey = leftKey
            rightBuffer = collectMatchingRightRows(rightKey)
            if (rightBuffer.nonEmpty) {
              nextOutput = Some(joinRows(leftRow, rightBuffer.head))
              rightBuffer = rightBuffer.tail
              return true
            } else {
              advanceLeft()
            }
          }
        } else {
          // We have buffered right rows with matching keys
          val leftRow = leftRowOpt.get._2
          nextOutput = Some(joinRows(leftRow, rightBuffer.head))
          rightBuffer = rightBuffer.tail
          if (rightBuffer.isEmpty) advanceLeft()
          return true
        }
      }

      false
    }

    override def next(): CypherRow = {
      if (!hasNext) throw new NoSuchElementException
      val result = nextOutput.get
      nextOutput = None
      result
    }

    private def advanceLeft(): Unit = {
      if (leftIter.hasNext) {
        leftRowOpt = Some(leftIter.next())
      } else {
        leftRowOpt = None
      }
    }

    private def advanceRight(): Unit = {
      if (rightIter.hasNext) {
        rightRowOpt = Some(rightIter.next())
      } else {
        rightRowOpt = None
      }
    }

    private def collectMatchingRightRows(key: Value): List[SlottedRow] = {
      val buffer = scala.collection.mutable.ListBuffer[SlottedRow]()
      while (rightRowOpt.isDefined && ValueComparator.compare(rightRowOpt.get._1, key) == 0) {
        buffer += rightRowOpt.get._2
        advanceRight()
      }
      buffer.toList
    }

    private def joinRows(leftRow: SlottedRow, rightRow: SlottedRow): CypherRow = {
      val newRow = SlottedRow(slots)
      newRow.copyAllFrom(leftRow)
      NodeHashJoinSlottedPipe.copyDataFromRow(rhsMappers, newRow, rightRow, state.query)
      newRow
    }
  }
}
