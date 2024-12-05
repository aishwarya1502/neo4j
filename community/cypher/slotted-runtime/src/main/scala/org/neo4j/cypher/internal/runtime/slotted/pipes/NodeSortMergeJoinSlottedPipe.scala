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

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapper
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMappers
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.math.Ordering

case class NodeSortMergeJoinSlottedPipe(
  lhsKeyOffsets: KeyOffsets,
  rhsKeyOffsets: KeyOffsets,
  left: Pipe,
  right: Pipe,
  slots: SlotConfiguration,
  rhsSlotMappings: SlotMappings
)(val id: Id = Id.INVALID_ID) extends Pipe {

  private val lhsOffsets: Array[Int] = lhsKeyOffsets.offsets
  private val lhsIsReference: Array[Boolean] = lhsKeyOffsets.isReference
  private val rhsOffsets: Array[Int] = rhsKeyOffsets.offsets
  private val rhsIsReference: Array[Boolean] = rhsKeyOffsets.isReference

  private val width: Int = lhsOffsets.length

  private val rhsMappers: Array[SlotMapper] = SlotMappers(rhsSlotMappings)

  // Define an implicit Ordering for Array[Long]
  implicit val longArrayOrdering: Ordering[Array[Long]] = (x: Array[Long], y: Array[Long]) => {
    // Compare arrays lexicographically (element by element)
    x.zip(y).collectFirst {
      case (a, b) if a != b => a.compareTo(b)
    }.getOrElse(0)
  }

  // Override internalCreateResults to return ClosingIterator[CypherRow]
  override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val lhsInput = left.createResults(state).map(_.asInstanceOf[SlottedRow]).toList
    val rhsInput = right.createResults(state).map(_.asInstanceOf[SlottedRow]).toList

    // Sort left and right inputs using the implicit longArrayOrdering
    val sortedLeft = lhsInput
      .filter(row => !hasNullKey(row, lhsOffsets, lhsIsReference))
      .sortBy(row => getKey(row, lhsOffsets, lhsIsReference))(longArrayOrdering)

    val sortedRight = rhsInput
      .filter(row => !hasNullKey(row, rhsOffsets, rhsIsReference))
      .sortBy(row => getKey(row, rhsOffsets, rhsIsReference))(longArrayOrdering)

    // Create an iterator for the results
    val resultIterator = new NodeSortMergeJoinIterator(
      sortedLeft.iterator,
      sortedRight.iterator,
      slots,
      rhsMappers,
      state
    )

    // Wrap the resultIterator in ClosingIterator to match the required return type
    ClosingIterator(resultIterator)
  }

  private def hasNullKey(row: SlottedRow, offsets: Array[Int], isReference: Array[Boolean]): Boolean = {
    var i = 0
    while (i < offsets.length) {
      val id = getNodeId(row, offsets(i), isReference(i))
      if (NullChecker.entityIsNull(id)) {
        return true
      }
      i += 1
    }
    false
  }

  private def getKey(row: SlottedRow, offsets: Array[Int], isReference: Array[Boolean]): Array[Long] = {
    val key = new Array[Long](offsets.length)
    var i = 0
    while (i < offsets.length) {
      key(i) = getNodeId(row, offsets(i), isReference(i))
      i += 1
    }
    key
  }

  private def getNodeId(row: SlottedRow, offset: Int, isRef: Boolean): Long = {
    if (isRef) {
      row.getRefAt(offset) match {
        case node: VirtualNodeValue => node.id()
        case _                      => NullChecker.NULL_ENTITY
      }
    } else {
      row.getLongAt(offset)
    }
  }

  private class NodeSortMergeJoinIterator(
    leftIter: Iterator[SlottedRow],
    rightIter: Iterator[SlottedRow],
    slots: SlotConfiguration,
    rhsMappers: Array[SlotMapper],
    state: QueryState
  ) extends Iterator[CypherRow] {

    private var leftRowOpt: Option[SlottedRow] = None
    private var rightRowOpt: Option[SlottedRow] = None
    private var rightBuffer: List[SlottedRow] = Nil
    private var currentLeftKey: Array[Long] = _
    private var nextOutput: Option[CypherRow] = None

    advanceLeft()
    advanceRight()

    override def hasNext: Boolean = {
      if (nextOutput.isDefined) return true

      while (leftRowOpt.isDefined && (rightRowOpt.isDefined || rightBuffer.nonEmpty)) {
        if (rightBuffer.isEmpty) {
          val leftKey = getKey(leftRowOpt.get, lhsOffsets, lhsIsReference)
          val rightKey = getKey(rightRowOpt.get, rhsOffsets, rhsIsReference)

          val cmp = compareKeys(leftKey, rightKey)
          if (cmp < 0) {
            advanceLeft()
          } else if (cmp > 0) {
            advanceRight()
          } else {
            // Matching keys
            currentLeftKey = leftKey
            rightBuffer = collectMatchingRightRows(rightKey)
            if (rightBuffer.nonEmpty) {
              nextOutput = Some(joinRows(leftRowOpt.get, rightBuffer.head))
              rightBuffer = rightBuffer.tail
              return true
            } else {
              advanceLeft()
            }
          }
        } else {
          // We have buffered right rows with matching keys
          nextOutput = Some(joinRows(leftRowOpt.get, rightBuffer.head))
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

    private def collectMatchingRightRows(key: Array[Long]): List[SlottedRow] = {
      val buffer = scala.collection.mutable.ListBuffer[SlottedRow]()
      while (rightRowOpt.isDefined && compareKeys(getKey(rightRowOpt.get, rhsOffsets, rhsIsReference), key) == 0) {
        buffer += rightRowOpt.get
        advanceRight()
      }
      buffer.toList
    }

    private def compareKeys(key1: Array[Long], key2: Array[Long]): Int = {
      longArrayOrdering.compare(key1, key2)
    }

    private def joinRows(leftRow: SlottedRow, rightRow: SlottedRow): CypherRow = {
      val newRow = SlottedRow(slots)
      newRow.copyAllFrom(leftRow)
      NodeHashJoinSlottedPipe.copyDataFromRow(rhsMappers, newRow, rightRow, state.query)
      newRow
    }
  }
}
