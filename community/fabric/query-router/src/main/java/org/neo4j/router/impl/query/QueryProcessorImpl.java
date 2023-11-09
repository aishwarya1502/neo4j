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
package org.neo4j.router.impl.query;

import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static scala.jdk.javaapi.OptionConverters.toJava;

import java.util.Optional;
import org.neo4j.cypher.internal.PreParsedQuery;
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.ast.AdministrationCommand;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.compiler.CypherParsing;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.frontend.phases.BaseState;
import org.neo4j.cypher.internal.tracing.CompilationTracer;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.internal.util.RecordingNotificationLogger;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.fabric.eval.StaticUseEvaluation;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

public class QueryProcessorImpl implements QueryProcessor {

    public static final CatalogName SYSTEM_DATABASE_CATALOG_NAME = CatalogName.of(SYSTEM_DATABASE_NAME);
    private final ProcessedQueryInfoCache cache;
    private final PreParser preParser;
    private final CypherParsing parsing;
    private final CompilationTracer tracer;
    private final CancellationChecker cancellationChecker;
    private final GlobalProcedures globalProcedures;
    private final StaticUseEvaluation staticUseEvaluation = new StaticUseEvaluation();

    public QueryProcessorImpl(
            ProcessedQueryInfoCache cache,
            PreParser preParser,
            CypherParsing parsing,
            CompilationTracer tracer,
            CancellationChecker cancellationChecker,
            GlobalProcedures globalProcedures) {
        this.cache = cache;
        this.preParser = preParser;
        this.parsing = parsing;
        this.tracer = tracer;
        this.cancellationChecker = cancellationChecker;
        this.globalProcedures = globalProcedures;
    }

    @Override
    public ProcessedQueryInfo processQuery(Query query, TargetService targetService) {
        var cachedValue = maybeGetFromCache(query, targetService);
        if (cachedValue != null) {
            return cachedValue;
        }

        return doProcessQuery(query, targetService);
    }

    @Override
    public long clearQueryCachesForDatabase(String databaseName) {
        return cache.clearQueryCachesForDatabase(databaseName);
    }

    private ProcessedQueryInfo maybeGetFromCache(Query query, TargetService targetService) {
        var cachedValue = cache.get(query.text());
        if (cachedValue == null) {
            return null;
        }

        try {
            var databaseReference = targetService.target(cachedValue.catalogInfo());
            // The database and alias info stored in System DB might have changed since
            // the value got cached.
            if (!databaseReference.equals(cachedValue.processedQueryInfo().target())) {
                cache.remove(query.text());
                return null;
            }
        } catch (DatabaseNotFoundException e) {
            // Or the alias is no longer there at all.
            cache.remove(query.text());
            throw e;
        }

        return cachedValue.processedQueryInfo();
    }

    private ProcessedQueryInfo doProcessQuery(Query query, TargetService targetService) {
        var queryTracer = tracer.compileQuery(query.text());
        var notificationLogger = new RecordingNotificationLogger();
        var preParsedQuery = preParser.preParse(query.text(), notificationLogger);
        var parsedQuery = parse(query, queryTracer, preParsedQuery);
        var catalogInfo = resolveCatalogInfo(parsedQuery.statement());
        var databaseReference = targetService.target(catalogInfo);
        var rewrittenQuery = maybeRewriteQuery(query, parsedQuery, databaseReference);
        var obfuscationMetadata = toJava(parsedQuery.maybeObfuscationMetadata());
        var resolver = SignatureResolver.from(globalProcedures.getCurrentView());
        var statementType = StatementType.of(parsedQuery.statement(), resolver);
        var cypherExecutionMode = preParsedQuery.options().queryOptions().executionMode();

        var processedQueryInfo = new ProcessedQueryInfo(
                databaseReference, rewrittenQuery, obfuscationMetadata, statementType, cypherExecutionMode);
        cache.put(query.text(), new ProcessedQueryInfoCache.Value(catalogInfo, processedQueryInfo));
        return processedQueryInfo;
    }

    private TargetService.CatalogInfo resolveCatalogInfo(Statement statement) {
        if (statement instanceof AdministrationCommand) {
            return new TargetService.SingleQueryCatalogInfo(Optional.of(SYSTEM_DATABASE_CATALOG_NAME));
        }

        var graphSelections = staticUseEvaluation.evaluateStaticTopQueriesGraphSelections(statement);
        return toCatalogInfo(graphSelections);
    }

    private Query maybeRewriteQuery(Query query, BaseState parsedQuery, DatabaseReference databaseReference) {
        if (databaseReference instanceof DatabaseReferenceImpl.External externalReference) {
            // TODO: this is where the magic for external references will happen
        }

        return query;
    }

    private BaseState parse(
            Query query, CompilationTracer.QueryCompilationEvent queryTracer, PreParsedQuery preParsedQuery) {
        return parsing.parseQuery(
                preParsedQuery.statement(),
                preParsedQuery.rawStatement(),
                new RecordingNotificationLogger(),
                preParsedQuery.options().queryOptions().planner().name(),
                Option.apply(preParsedQuery.options().offset()),
                queryTracer,
                query.parameters(),
                cancellationChecker);
    }

    private TargetService.CatalogInfo toCatalogInfo(Seq<Option<CatalogName>> graphSelections) {
        if (graphSelections.size() == 1) {
            return new TargetService.SingleQueryCatalogInfo(OptionConverters.toJava(graphSelections.head()));
        }

        var catalogNames = CollectionConverters.asJava(graphSelections).stream()
                .map(OptionConverters::toJava)
                .toList();
        return new TargetService.UnionQueryCatalogInfo(catalogNames);
    }
}