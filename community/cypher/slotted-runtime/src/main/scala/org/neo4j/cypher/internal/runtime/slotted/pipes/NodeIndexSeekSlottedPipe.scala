/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EntityIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class NodeIndexSeekSlottedPipe(
  ident: String,
  label: LabelToken,
  properties: IndexedSeq[SlottedIndexedProperty],
  queryIndexId: Int,
  valueExpr: QueryExpression[Expression],
  indexMode: IndexSeekMode,
  indexOrder: IndexOrder,
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID) extends Pipe with EntityIndexSeeker with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId).toArray

  override val indexPropertyIndices: Array[Int] =
    properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray

  override val indexPropertySlotOffsets: Array[Int] =
    properties.map(_.maybeCachedEntityPropertySlot).collect { case Some(o) => o }.toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val index = state.queryIndexes(queryIndexId)
    val context = state.newRowWithArgument(rowFactory)
    new SlottedNodeIndexIterator(state, indexSeek(state, index, needsValues, indexOrder, context))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NodeIndexSeekSlottedPipe]

  override def equals(other: Any): Boolean = other match {
    case that: NodeIndexSeekSlottedPipe =>
      (that canEqual this) &&
      ident == that.ident &&
      label == that.label &&
      (properties == that.properties) &&
      valueExpr == that.valueExpr &&
      indexMode == that.indexMode &&
      slots == that.slots
    case _ => false
  }

  override def hashCode(): Int = {
    val state: Seq[Object] = Seq(ident, label, properties, valueExpr, indexMode, slots)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}