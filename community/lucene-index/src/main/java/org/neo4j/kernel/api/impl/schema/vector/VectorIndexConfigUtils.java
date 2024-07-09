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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.INVALID_STATES;
import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.VALID;
import static org.neo4j.internal.schema.IndexConfigValidationWrapper.unrecognizedSetting;

import java.util.Objects;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.map.ImmutableMap;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexConfigValidationRecords;
import org.neo4j.internal.schema.IndexConfigValidationRecords.IncorrectType;
import org.neo4j.internal.schema.IndexConfigValidationRecords.IndexConfigValidationRecord;
import org.neo4j.internal.schema.IndexConfigValidationRecords.InvalidValue;
import org.neo4j.internal.schema.IndexConfigValidationRecords.Valid;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.PrettyPrinter;

public class VectorIndexConfigUtils {
    static final IndexSetting DIMENSIONS = IndexSetting.vector_Dimensions();
    static final IndexSetting SIMILARITY_FUNCTION = IndexSetting.vector_Similarity_Function();

    public record Range<T extends Comparable<T>>(T min, T max) {
        public Range {
            Preconditions.checkArgument(
                    Objects.requireNonNull(min).compareTo(Objects.requireNonNull(max)) <= 0,
                    "min must be less than or equal to max");
        }

        public boolean contains(T value) {
            Objects.requireNonNull(value);
            return min.compareTo(value) <= 0 && max.compareTo(value) >= 0;
        }

        public boolean isBefore(T value) {
            Objects.requireNonNull(value);
            return value.compareTo(max) > 0;
        }

        public boolean isAfter(T value) {
            Objects.requireNonNull(value);
            return value.compareTo(min) < 0;
        }
    }

    static boolean missing(AnyValue value) {
        return value == null || value == Values.NO_VALUE;
    }

    static ImmutableMap<IndexSetting, Object> toValidSettings(RichIterable<Valid> validRecords) {
        return validRecords.toImmutableMap(Valid::setting, Valid::value);
    }

    static IndexConfig toIndexConfig(RichIterable<Valid> validRecords) {
        return toIndexConfig(validRecords, valid -> true);
    }

    static IndexConfig toIndexConfig(RichIterable<Valid> validRecords, RichIterable<IndexSetting> filter) {
        return toIndexConfig(validRecords, valid -> filter.contains(valid.setting()));
    }

    static IndexConfig toIndexConfig(RichIterable<Valid> validRecords, Predicate<Valid> filter) {
        return IndexConfig.with(validRecords
                .asLazy()
                .select(filter)
                .toMap(valid -> valid.setting().getSettingName(), Valid::stored));
    }

    static void assertValidRecords(
            IndexConfigValidationRecords validationRecords,
            IndexProviderDescriptor descriptor,
            Iterable<String> validSettingNames) {
        // fail on first
        final var invalidRecord =
                INVALID_STATES.asLazy().flatCollect(validationRecords::get).getFirst();
        if (invalidRecord == null) {
            return;
        }

        // When we can rely on Java 21, might be worth refactoring to use
        // JEP 441: Pattern Matching for switch
        final var settingName = invalidRecord.settingName();
        throw switch (invalidRecord.state()) {
                // this is a logic error
            case VALID -> new IllegalStateException("%s should not be %s at. Provided: %s"
                    .formatted(IndexConfigValidationRecord.class.getSimpleName(), VALID, invalidRecord));

                // this is an implementation mistake
            case PENDING -> new IllegalStateException("Validation for '%s' is incomplete.".formatted(settingName));

                // these are likely user mistakes
            case UNRECOGNIZED_SETTING -> unrecognizedSetting(
                    invalidRecord.settingName(), descriptor, validSettingNames);
            case MISSING_SETTING -> new IllegalArgumentException(
                    "'%s' is expected to have been set".formatted(settingName));
            case INCORRECT_TYPE -> {
                final var incorrectType = (IncorrectType) invalidRecord;
                yield new IllegalArgumentException("'%s' is expected to have been '%s', but was '%s'"
                        .formatted(settingName, incorrectType.targetType(), incorrectType.providedType()));
            }
            case INVALID_VALUE -> {
                final var invalidValue = (InvalidValue) invalidRecord;
                final var valid = invalidValue.valid();
                if (valid instanceof final Range<?> range) {
                    yield new IllegalArgumentException("'%s' must be between %s and %s inclusively"
                            .formatted(settingName, range.min(), range.max()));
                } else if (valid instanceof final Iterable<?> iterable) {
                    final var pp = new PrettyPrinter();
                    invalidValue.rawValue().writeTo(pp);
                    yield new IllegalArgumentException(
                            "'%s' is an unsupported '%s'. Supported: %s".formatted(pp.value(), settingName, iterable));
                }

                // this is an implementation mistake
                yield new IllegalStateException("Unhandled valid value type '%s' for '%s'. Provided: %s"
                        .formatted(valid.getClass().getSimpleName(), settingName, valid));
            }
        };
    }
}