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
package org.neo4j.internal.batchimport.input.parquet;

import static org.neo4j.util.Preconditions.checkState;

import blue.strategic.parquet.ParquetReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.HeaderException;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenConstants;

/**
 * Provides {@link Input} from data contained in Parquet files.
 */
public class ParquetInput implements Input {

    private final ParquetDataFactory nodeDataFactory;
    private final ParquetDataFactory relationshipDataFactory;
    private final IdType idType;
    private final Groups groups;
    private final ParquetMonitor monitor;
    private final Map<Path, List<ParquetColumn>> columnInfo;
    private final Map<Set<String>, List<Path[]>> nodeFiles;
    private final Map<String, List<Path[]>> relationshipFiles;
    private final String arrayDelimiter;

    public ParquetInput(
            Map<Set<String>, List<Path[]>> nodeFiles,
            Map<String, List<Path[]>> relationshipFiles,
            IdType idType,
            Character arrayDelimiter,
            Groups groups,
            ParquetMonitor monitor) {
        this.idType = idType;
        this.groups = groups;
        this.monitor = monitor;
        this.arrayDelimiter = arrayDelimiter.toString();
        this.nodeFiles = nodeFiles;
        this.relationshipFiles = relationshipFiles;
        this.nodeDataFactory = new ParquetDataFactory(nodeFiles.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stream()
                        .flatMap(Arrays::stream)
                        .toList())));
        this.relationshipDataFactory = new ParquetDataFactory(relationshipFiles.entrySet().stream()
                .collect(Collectors.toMap(entry -> Set.of(entry.getKey()), entry -> entry.getValue().stream()
                        .flatMap(Arrays::stream)
                        .toList())));

        this.columnInfo = verifyColumns(nodeFiles, relationshipFiles);
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () ->
                new ParquetGroupInputIterator(nodeDataFactory, groups, idType, arrayDelimiter, columnInfo, monitor);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new ParquetGroupInputIterator(
                relationshipDataFactory, groups, idType, arrayDelimiter, columnInfo, monitor);
    }

    @Override
    public IdType idType() {
        return idType;
    }

    @Override
    public ReadableGroups groups() {
        return groups;
    }

    private Map<Path, List<ParquetColumn>> verifyColumns(
            Map<Set<String>, List<Path[]>> labelsAndNodeFiles, Map<String, List<Path[]>> typeAndRelationshipFiles) {
        Map<Path, List<ParquetColumn>> columnInfo = new HashMap<>();
        try {
            for (Map.Entry<Set<String>, List<Path[]>> labelsAndNodeFilesEntry : labelsAndNodeFiles.entrySet()) {
                var hasLabelColumn = !labelsAndNodeFilesEntry.getKey().isEmpty()
                        && labelsAndNodeFilesEntry.getKey().stream().anyMatch(label -> !label.isBlank());
                var nodeFiles = labelsAndNodeFilesEntry.getValue().stream()
                        .flatMap(Arrays::stream)
                        .toList();
                for (Path nodeFile : nodeFiles) {
                    var currentColumnInfo = new ArrayList<ParquetColumn>();
                    var propertyNames = new HashSet<String>();
                    String previousGroupName = null;
                    var metadata = ParquetReader.readMetadata(nodeFile.toFile());
                    var columns = metadata.getFileMetaData().getSchema().getColumns();
                    // check for possible group / ID space definitions and register them
                    String fileName = nodeFile.getFileName().toString();
                    for (ColumnDescriptor columnDescriptor : columns) {
                        var columnName = columnDescriptor.getPath()[0];
                        if (columnName.isBlank()) {
                            throw new InputException("column name is must not be blank");
                        }
                        try {
                            var parquetColumn = ParquetColumn.from(columnName, EntityType.NODE);
                            String propertyName = parquetColumn.propertyName() != null
                                    ? parquetColumn.propertyName()
                                    : parquetColumn.logicalColumnType().name();
                            if (parquetColumn.isIdColumn() && parquetColumn.groupName() != null) {
                                if (previousGroupName != null && !previousGroupName.equals(parquetColumn.groupName())) {
                                    throw new IllegalStateException(
                                            "There are multiple :ID columns, but they are referring to different groups");
                                }
                                previousGroupName = parquetColumn.groupName();
                            }
                            if (propertyNames.contains(propertyName) && parquetColumn.isIdColumn()) {
                                throw new DuplicatedColumnException(
                                        "Cannot store composite IDs as properties, only individual part. Property %s / File: %s"
                                                .formatted(propertyName, fileName));
                            }
                            if (propertyNames.contains(propertyName)) {
                                throw new DuplicatedColumnException("Duplicated header property %s found in file %s."
                                        .formatted(propertyName, fileName));
                            }
                            propertyNames.add(propertyName);
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.ID) {
                                groups.getOrCreate(parquetColumn.groupName());
                            }
                            if (parquetColumn.columnType().needsConversion()) {
                                monitor.typeNormalized(
                                        fileName,
                                        propertyName,
                                        parquetColumn.columnType().name(),
                                        parquetColumn
                                                .columnType()
                                                .convertedType()
                                                .name());
                            }
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.LABEL) {
                                hasLabelColumn = true;
                            }
                            currentColumnInfo.add(parquetColumn);
                        } catch (IllegalArgumentException e) {
                            throw new InputException("Column name " + columnName
                                    + " is used as a special type but is unknown. Allowed types are "
                                    + ParquetColumn.getReservedColumns(EntityType.NODE));
                        }
                    }
                    if (!hasLabelColumn) {
                        monitor.noNodeLabelsSpecified(fileName);
                    }
                    columnInfo.put(nodeFile, currentColumnInfo);
                }
            }

            for (Map.Entry<String, List<Path[]>> typeAndRelationshipFilesEntry : typeAndRelationshipFiles.entrySet()) {

                // parse all relationship headers and verify all ID spaces
                var relationshipFileList = typeAndRelationshipFilesEntry.getValue().stream()
                        .flatMap(Arrays::stream)
                        .toList();
                for (Path relationshipFile : relationshipFileList) {
                    var currentColumnInfo = new ArrayList<ParquetColumn>();
                    var propertyNames = new HashSet<String>();
                    var metadata = ParquetReader.readMetadata(relationshipFile.toFile());
                    var columns = metadata.getFileMetaData().getSchema().getColumns();
                    var hasTypeColumn = typeAndRelationshipFilesEntry.getKey() != null
                            && !typeAndRelationshipFilesEntry.getKey().isBlank();
                    String fileName = relationshipFile.getFileName().toString();
                    for (ColumnDescriptor columnDescriptor : columns) {
                        var columnName = columnDescriptor.getPath()[0];
                        try {
                            var parquetColumn = ParquetColumn.from(columnName, EntityType.RELATIONSHIP);
                            String propertyName = parquetColumn.propertyName() != null
                                    ? parquetColumn.propertyName()
                                    : parquetColumn.logicalColumnType().name();
                            if (propertyNames.contains(propertyName)) {
                                throw new DuplicatedColumnException("Duplicated header property %s found in file %s."
                                        .formatted(propertyName, fileName));
                            }
                            propertyNames.add(propertyName);
                            if (parquetColumn.columnType().needsConversion()) {
                                monitor.typeNormalized(
                                        fileName,
                                        propertyName,
                                        parquetColumn.columnType().name(),
                                        parquetColumn
                                                .columnType()
                                                .convertedType()
                                                .name());
                            }
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.START_ID
                                    || parquetColumn.logicalColumnType() == ParquetLogicalColumnType.END_ID) {
                                try {
                                    groups.get(parquetColumn.groupName());
                                } catch (HeaderException e) {
                                    throw new InputException(e.getMessage());
                                }
                            }
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.TYPE) {
                                hasTypeColumn = true;
                            }
                            currentColumnInfo.add(parquetColumn);
                        } catch (IllegalArgumentException e) {
                            throw new InputException("Column name " + columnName
                                    + " is used as a special type but is unknown. Allowed types are "
                                    + ParquetColumn.getReservedColumns(EntityType.RELATIONSHIP));
                        }
                    }
                    columnInfo.put(relationshipFile, currentColumnInfo);

                    if (!hasTypeColumn) {
                        monitor.noRelationshipTypeSpecified(fileName);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return columnInfo;
    }

    @Override
    public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        List<ParquetColumn> idColumns = columnInfo.values().stream()
                .flatMap(Collection::stream)
                .filter(ParquetColumn::isIdColumn)
                .toList();
        HashMap<String, SchemaDescriptor> result = new HashMap<>();
        checkReferencedNodeSchema(idColumns, tokenHolders, result);
        return result;
    }

    private void checkReferencedNodeSchema(
            List<ParquetColumn> idColumns, TokenHolders tokenHolders, Map<String, SchemaDescriptor> result) {
        idColumns.forEach(column -> {
            var labelName = column.idLabel();
            checkState(labelName != null, "No label was specified for the node index in '%s'", column);
            var keyName = column.propertyName();
            checkState(keyName != null, "No property key was specified for node index in '%s'", column);
            var label = tokenHolders.labelTokens().getIdByName(labelName);
            var key = tokenHolders.propertyKeyTokens().getIdByName(keyName);
            checkState(
                    label != TokenConstants.NO_TOKEN,
                    "Label '%s' for node index specified in '%s' does not exist",
                    labelName,
                    column);
            checkState(
                    key != TokenConstants.NO_TOKEN,
                    "Property key '%s' for node index specified in '%s' does not exist",
                    keyName,
                    column);
            var schemaDescriptor = SchemaDescriptors.forLabel(label, key);
            var prev = result.put(column.groupName(), schemaDescriptor);
            checkState(
                    prev == null || prev.equals(schemaDescriptor),
                    "Multiple different indexes for group " + column.groupName());
        });
    }

    @Override
    public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator) throws IOException {
        // fly over node files
        long numberOfNodes = 0;
        long numberOfNodeProperties = 0;
        long totalNodePropertiesSize = 0;
        Set<String> mergedLabels = new HashSet<>();
        for (Map.Entry<Set<String>, List<Path[]>> nodePathEntries : nodeFiles.entrySet()) {
            mergedLabels.addAll(Collections.unmodifiableSet(nodePathEntries.getKey()));
            for (Path[] nodePaths : nodePathEntries.getValue()) {
                for (Path nodePath : nodePaths) {
                    var metadata = ParquetReader.readMetadata(nodePath.toFile());
                    List<BlockMetaData> blocks = metadata.getBlocks();
                    for (BlockMetaData block : blocks) {
                        numberOfNodes += block.getRowCount();
                        var currentColumnCount = block.getColumns().size();
                        // This needs to be separated by file/group, or?
                        if (currentColumnCount > numberOfNodeProperties) {
                            numberOfNodeProperties = currentColumnCount;
                        }
                        for (ColumnChunkMetaData column : block.getColumns()) {
                            totalNodePropertiesSize += column.getTotalUncompressedSize();
                        }
                    }
                }
            }
        }
        var numberOfNodeLabels = mergedLabels.size();

        // fly over relationship files
        long numberOfRelationships = 0;
        long numberOfRelationshipProperties = 0;
        long totalRelationshipPropertiesSize = 0;
        for (Map.Entry<String, List<Path[]>> relationshipFileEntries : relationshipFiles.entrySet()) {
            for (Path[] relationshipPaths : relationshipFileEntries.getValue()) {
                for (Path relationshipPath : relationshipPaths) {
                    var metadata = ParquetReader.readMetadata(relationshipPath.toFile());
                    for (BlockMetaData block : metadata.getBlocks()) {
                        numberOfNodes += block.getRowCount();
                        var currentColumnCount = block.getColumns().size();
                        if (currentColumnCount > numberOfNodeProperties) {
                            numberOfNodeProperties = currentColumnCount;
                        }
                        for (ColumnChunkMetaData column : block.getColumns()) {
                            totalRelationshipPropertiesSize += column.getTotalUncompressedSize();
                        }
                    }
                }
            }
        }

        return new Estimates(
                numberOfNodes,
                numberOfRelationships,
                numberOfNodeProperties,
                numberOfRelationshipProperties,
                totalNodePropertiesSize,
                totalRelationshipPropertiesSize,
                numberOfNodeLabels);
    }
}