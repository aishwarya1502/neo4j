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
package org.neo4j.internal.schema;

import java.util.Set;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;

/**
 * Note: ordering is defined by CIP-90.
 */
public enum SchemaValueType {
    BOOLEAN("BOOLEAN", ValueRepresentation.BOOLEAN),
    STRING("STRING", ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT),
    INTEGER(
            "INTEGER",
            ValueRepresentation.INT64,
            ValueRepresentation.INT32,
            ValueRepresentation.INT16,
            ValueRepresentation.INT8),
    FLOAT("FLOAT", ValueRepresentation.FLOAT64, ValueRepresentation.FLOAT32),
    DURATION("DURATION", ValueRepresentation.DURATION),
    DATE("DATE", ValueRepresentation.DATE),
    ZONED_DATETIME("ZONED DATETIME", ValueRepresentation.ZONED_DATE_TIME),
    LOCAL_DATETIME("LOCAL DATETIME", ValueRepresentation.LOCAL_DATE_TIME),
    ZONED_TIME("ZONED TIME", ValueRepresentation.ZONED_TIME),
    LOCAL_TIME("LOCAL TIME", ValueRepresentation.LOCAL_TIME),
    POINT("POINT", ValueRepresentation.GEOMETRY),

    LIST_BOOLEAN("LIST<BOOLEAN>", ValueRepresentation.BOOLEAN_ARRAY),
    LIST_STRING("LIST<STRING>", ValueRepresentation.TEXT_ARRAY),
    LIST_INTEGER(
            "LIST<INTEGER>",
            ValueRepresentation.INT64_ARRAY,
            ValueRepresentation.INT32_ARRAY,
            ValueRepresentation.INT16_ARRAY,
            ValueRepresentation.INT8_ARRAY),
    LIST_FLOAT("LIST<FLOAT>", ValueRepresentation.FLOAT64_ARRAY, ValueRepresentation.FLOAT32_ARRAY),
    LIST_DURATION("LIST<DURATION>", ValueRepresentation.DURATION_ARRAY),
    LIST_DATE("LIST<DATE>", ValueRepresentation.DATE_ARRAY),
    LIST_ZONED_DATETIME("LIST<ZONED DATETIME>", ValueRepresentation.ZONED_DATE_TIME_ARRAY),
    LIST_LOCAL_DATETIME("LIST<LOCAL DATETIME>", ValueRepresentation.LOCAL_DATE_TIME_ARRAY),
    LIST_ZONED_TIME("LIST<ZONED TIME>", ValueRepresentation.ZONED_TIME_ARRAY),
    LIST_LOCAL_TIME("LIST<LOCAL TIME>", ValueRepresentation.LOCAL_TIME_ARRAY),
    LIST_POINT("LIST<POINT>", ValueRepresentation.GEOMETRY_ARRAY);

    private final String userDescription;
    private final Set<ValueRepresentation> valueRepresentations;

    SchemaValueType(String userDescription, ValueRepresentation... valueRepresentations) {
        this.userDescription = userDescription;
        this.valueRepresentations = Set.of(valueRepresentations);
    }

    public boolean isAssignable(Value value) {
        return valueRepresentations.contains(value.valueRepresentation());
    }

    public boolean isAssignable(ValueRepresentation valueRepresentation) {
        return valueRepresentations.contains(valueRepresentation);
    }

    public String userDescription() {
        return userDescription;
    }

    public String serialize() {
        return this.name();
    }

    @Override
    public String toString() {
        return userDescription;
    }
}