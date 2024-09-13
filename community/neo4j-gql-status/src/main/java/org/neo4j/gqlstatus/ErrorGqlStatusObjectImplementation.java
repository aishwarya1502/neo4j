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
package org.neo4j.gqlstatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ErrorGqlStatusObjectImplementation extends CommonGqlStatusObjectImplementation
        implements ErrorGqlStatusObject {
    private final Optional<ErrorGqlStatusObject> cause;
    private final Map<GqlParams.GqlParam, Object> paramMap;
    private final GqlStatusInfoCodes gqlStatusInfoCode;

    private ErrorGqlStatusObjectImplementation(
            GqlStatusInfoCodes gqlStatusInfoCode,
            Map<GqlParams.GqlParam, Object> parameters,
            ErrorGqlStatusObject cause,
            DiagnosticRecord diagnosticRecord) {
        super(gqlStatusInfoCode, diagnosticRecord, parameters);
        this.gqlStatusInfoCode = gqlStatusInfoCode;
        this.cause = Optional.ofNullable(cause);
        this.paramMap = Map.copyOf(parameters);
    }

    public static Builder from(GqlStatusInfoCodes gqlStatusInfo) {
        return new Builder(gqlStatusInfo);
    }

    @Override
    public Optional<ErrorGqlStatusObject> cause() {
        return cause;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return this;
    }

    @Override
    public String legacyMessage() {
        return "";
    }

    @Override
    public String toString() {
        return recToString();
    }

    private String recToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("Status: ");
        sb.append(gqlStatusInfoCode.getStatusString().trim());
        sb.append("\n");
        sb.append("Message: ");
        sb.append(insertMessageParameters(paramMap).trim());
        sb.append("\n");
        sb.append("Subcondition: ");
        sb.append(gqlStatusInfoCode.getSubCondition().trim());
        if (cause.isPresent()) {
            sb.append("\n");
            sb.append("Caused by:");
            return sb.append(cause.get().toString().indent(4)).toString();
        } else {
            return sb.toString();
        }
    }

    public static class Builder {
        private ErrorGqlStatusObject cause = null;
        private final Map<GqlParams.GqlParam, Object> paramMap = new HashMap<>();
        private final GqlStatusInfoCodes gqlStatusInfoCode;
        private final DiagnosticRecord.Builder diagnosticRecordBuilder = DiagnosticRecord.from();

        private Builder(GqlStatusInfoCodes gqlStatusInfo) {
            this.gqlStatusInfoCode = gqlStatusInfo;
        }

        public Builder withParam(GqlParams.StringParam param, String value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.BooleanParam param, boolean value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.NumberParam param, Number value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withParam(GqlParams.ListParam param, List<?> value) {
            this.paramMap.put(param, value);
            return this;
        }

        public Builder withCause(ErrorGqlStatusObject cause) {
            this.cause = cause;
            return this;
        }

        public Builder withClassification(GqlClassification classification) {
            diagnosticRecordBuilder.withClassification(classification);
            return this;
        }

        public Builder atPosition(int line, int col, int offset) {
            diagnosticRecordBuilder.atPosition(line, col, offset);
            return this;
        }

        public ErrorGqlStatusObject build() {
            DiagnosticRecord diagnosticRecord = diagnosticRecordBuilder.build();
            return new ErrorGqlStatusObjectImplementation(gqlStatusInfoCode, paramMap, cause, diagnosticRecord);
        }
    }
}
