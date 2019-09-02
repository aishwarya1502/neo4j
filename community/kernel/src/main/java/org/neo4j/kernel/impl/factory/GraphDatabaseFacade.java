/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.factory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultConsumer;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.graphdb.ResultConsumer.EMPTY_CONSUMER;
import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.values.storable.Values.utf8Value;

/**
 * Implementation of the GraphDatabaseService interfaces - the "Core API".
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI, EmbeddedProxySPI
{
    private final Schema schema;
    private final Database database;
    private final ThreadToStatementContextBridge statementContext;
    private final TransactionalContextFactory contextFactory;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final DatabaseInfo databaseInfo;
    private Function<LoginContext, LoginContext> loginContextTransformer = Function.identity();
    static final ThreadLocal<TopLevelTransaction> TEMP_TOP_LEVEL_TRANSACTION = new ThreadLocal<>();

    public GraphDatabaseFacade( GraphDatabaseFacade facade, Function<LoginContext,LoginContext> loginContextTransformer )
    {
        this( facade.database, facade.statementContext, facade.config, facade.databaseInfo, facade.availabilityGuard );
        this.loginContextTransformer = requireNonNull( loginContextTransformer );
    }

    public GraphDatabaseFacade( Database database, ThreadToStatementContextBridge txBridge, Config config, DatabaseInfo databaseInfo,
            DatabaseAvailabilityGuard availabilityGuard )
    {
        this.database = requireNonNull( database );
        this.config = requireNonNull( config );
        this.statementContext = requireNonNull( txBridge );
        this.availabilityGuard = requireNonNull( availabilityGuard );
        this.databaseInfo = requireNonNull( databaseInfo );
        this.schema = new SchemaImpl( () -> txBridge.getKernelTransactionBoundToThisThread( true, databaseId() ) );
        this.tokenHolders = database.getTokenHolders();
        this.contextFactory = Neo4jTransactionalContextFactory.create( this,
                () -> getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class ),
                new FacadeKernelTransactionFactory( config, this ),
                txBridge );
    }

    @Override
    public Node getNodeById( long id )
    {
        if ( id < 0 )
        {
            throw new NotFoundException( format( "Node %d not found", id ),
                    new EntityNotFoundException( EntityType.NODE, id ) );
        }

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        assertTransactionOpen( ktx );
        try ( Statement ignore = ktx.acquireStatement() )
        {
            if ( !ktx.dataRead().nodeExists( id ) )
            {
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            }
            return newNodeProxy( id );
        }
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 )
        {
            throw new NotFoundException( format( "Relationship %d not found", id ),
                    new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
        }

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        assertTransactionOpen( ktx );
        try ( Statement ignore = ktx.acquireStatement() )
        {
            if ( !ktx.dataRead().relationshipExists( id ) )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ),
                        new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
            }
            return newRelationshipProxy( id );
        }
    }

    @Override
    public Schema schema()
    {
        assertTransactionOpen();
        return schema;
    }

    @Override
    public boolean isAvailable( long timeoutMillis )
    {
        return database.getDatabaseAvailabilityGuard().isAvailable( timeoutMillis );
    }

    @Override
    public Transaction beginTx()
    {
        return beginTransaction();
    }

    protected InternalTransaction beginTransaction()
    {
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit unit )
    {
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout, unit );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext )
    {
        return beginTransaction( type, loginContext, EMBEDDED_CONNECTION );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, config.get( transaction_timeout ).toMillis() );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout,
            TimeUnit unit )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, unit.toMillis( timeout ) );
    }

    @Override
    public void executeTransactionally( String query ) throws QueryExecutionException
    {
        executeTransactionally( query, emptyMap(), EMPTY_CONSUMER );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters, ResultConsumer resultConsumer ) throws QueryExecutionException
    {
        executeTransactionally( query, parameters, resultConsumer, config.get( transaction_timeout ) );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters, ResultConsumer resultConsumer, Duration timeout )
            throws QueryExecutionException
    {
        try ( var internalTransaction = beginTransaction( implicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout.toMillis(), MILLISECONDS ) )
        {
            try ( var result = execute( query, parameters ) )
            {
                resultConsumer.accept( result );
            }
            internalTransaction.commit();
        }
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, emptyMap() );
    }

    @Override
    public Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return execute( query, emptyMap(), timeout, unit );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        TopLevelTransaction transaction = TEMP_TOP_LEVEL_TRANSACTION.get();
        return execute( transaction, query, ValueUtils.asParameterMapValue( parameters ) );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws
            QueryExecutionException
    {
        TopLevelTransaction transaction = TEMP_TOP_LEVEL_TRANSACTION.get();
        return execute( transaction, query, ValueUtils.asParameterMapValue( parameters ) );
    }

    private Result execute( InternalTransaction transaction, String query, MapValue parameters )
            throws QueryExecutionException
    {
        TransactionalContext context = contextFactory.newContext( transaction, query, parameters );
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            return database.getExecutionEngine().executeQuery( query, parameters, context, false );
        }
        catch ( UnavailableException ue )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( ue.getMessage(), ue );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        assertTransactionOpen( ktx );
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            NodeCursor cursor = ktx.cursors().allocateNodeCursor();
            ktx.dataRead().allNodesScan( cursor );
            return new PrefetchingResourceIterator<>()
            {
                @Override
                protected Node fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newNodeProxy( cursor.nodeReference() );
                    }
                    else
                    {
                        close();
                        return null;
                    }
                }

                @Override
                public void close()
                {
                    cursor.close();
                    statement.close();
                }
            };
        };
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        assertTransactionOpen( ktx );
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            RelationshipScanCursor cursor = ktx.cursors().allocateRelationshipScanCursor();
            ktx.dataRead().allRelationshipsScan( cursor );
            return new PrefetchingResourceIterator<>()
            {
                @Override
                protected Relationship fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newRelationshipProxy( cursor.relationshipReference(), cursor.sourceNodeReference(), cursor.type(),
                                cursor.targetNodeReference() );
                    }
                    else
                    {
                        close();
                        return null;
                    }
                }

                @Override
                public void close()
                {
                    cursor.close();
                    statement.close();
                }
            };
        };
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse()
    {
        return allInUse( TokenAccess.LABELS );
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return allInUse( TokenAccess.RELATIONSHIP_TYPES );
    }

    private <T> ResourceIterable<T> allInUse( final TokenAccess<T> tokens )
    {
        assertTransactionOpen();
        return () -> tokens.inUse( statementContext.getKernelTransactionBoundToThisThread( true, databaseId() ) );
    }

    @Override
    public ResourceIterable<Label> getAllLabels()
    {
        return all( TokenAccess.LABELS );
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes()
    {
        return all( TokenAccess.RELATIONSHIP_TYPES );
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys()
    {
        return all( TokenAccess.PROPERTY_KEYS );
    }

    private <T> ResourceIterable<T> all( final TokenAccess<T> tokens )
    {
        assertTransactionOpen();
        return () ->
        {
            KernelTransaction transaction =
                    statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
            return tokens.all( transaction );
        };
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel, final String key, final Object value )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( myLabel.name() );
        int propertyId = tokenRead.propertyKey( key );
        return nodesByLabelAndProperty( transaction, labelId, IndexQuery.exact( propertyId, Values.of( value ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        return nodesByLabelAndProperties( transaction, labelId,
                                          IndexQuery.exact( tokenRead.propertyKey( key1 ), Values.of( value1 ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key2 ), Values.of( value2 ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2,
            String key3, Object value3 )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        return nodesByLabelAndProperties( transaction, labelId,
                                          IndexQuery.exact( tokenRead.propertyKey( key1 ), Values.of( value1 ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key2 ), Values.of( value2 ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key3 ), Values.of( value3 ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, Map<String,Object> propertyValues )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        IndexQuery.ExactPredicate[] queries = new IndexQuery.ExactPredicate[propertyValues.size()];
        int i = 0;
        for ( Map.Entry<String,Object> entry : propertyValues.entrySet() )
        {
            queries[i++] = IndexQuery.exact( tokenRead.propertyKey( entry.getKey() ), Values.of( entry.getValue() ) );
        }
        return nodesByLabelAndProperties( transaction, labelId, queries );
    }

    @Override
    public ResourceIterator<Node> findNodes(
            final Label myLabel, final String key, final String value, final StringSearchMode searchMode )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( myLabel.name() );
        int propertyId = tokenRead.propertyKey( key );
        IndexQuery query;
        switch ( searchMode )
        {
        case EXACT:
            query = IndexQuery.exact( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case PREFIX:
            query = IndexQuery.stringPrefix( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case SUFFIX:
            query = IndexQuery.stringSuffix( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case CONTAINS:
            query = IndexQuery.stringContains( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        default:
            throw new IllegalStateException( "Unknown string search mode: " + searchMode );
        }
        return nodesByLabelAndProperty( transaction, labelId, query );
    }

    @Override
    public Node findNode( final Label myLabel, final String key, final Object value )
    {
        try ( ResourceIterator<Node> iterator = findNodes( myLabel, key, value ) )
        {
            if ( !iterator.hasNext() )
            {
                return null;
            }
            Node node = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new MultipleFoundException(
                        format( "Found multiple nodes with label: '%s', property name: '%s' and property " +
                                "value: '%s' while only one was expected.", myLabel, key, value ) );
            }
            return node;
        }
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel )
    {
        return allNodesWithLabel( myLabel );
    }

    private InternalTransaction beginTransactionInternal( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo,
            long timeoutMillis )
    {
        if ( statementContext.hasTransaction() )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Fail to start new transaction. Already have transaction in the context." );
        }
        final KernelTransaction kernelTransaction = beginKernelTransaction( type, loginContext, connectionInfo, timeoutMillis );
        TopLevelTransaction transaction = new TopLevelTransaction( this, kernelTransaction, TEMP_TOP_LEVEL_TRANSACTION );
        TEMP_TOP_LEVEL_TRANSACTION.set( transaction );
        return transaction;
    }

    @Override
    public DatabaseId databaseId()
    {
        return database.getDatabaseId();
    }

    KernelTransaction beginKernelTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo,
            long timeout )
    {
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            KernelTransaction kernelTx = database.getKernel().beginTransaction( type, loginContextTransformer.apply( loginContext ), connectionInfo, timeout );
            kernelTx.registerCloseListener( txId -> statementContext.unbindTransactionFromCurrentThread() );
            statementContext.bindTransactionToCurrentThread( kernelTx );
            return kernelTx;
        }
        catch ( UnavailableException | TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }

    private ResourceIterator<Node> nodesByLabelAndProperty( KernelTransaction transaction, int labelId, IndexQuery query )
    {
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();

        if ( query.propertyKeyId() == TokenRead.NO_TOKEN || labelId == TokenRead.NO_TOKEN )
        {
            statement.close();
            return emptyResourceIterator();
        }
        IndexDescriptor index = transaction.schemaRead().index( labelId, query.propertyKeyId() );
        if ( index != IndexDescriptor.NO_INDEX )
        {
            // Ha! We found an index - let's use it to find matching nodes
            try
            {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                IndexReadSession indexSession = read.indexReadSession( index );
                read.nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, query );

                return new NodeCursorResourceIterator<>( cursor, statement, this::newNodeProxy );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutIndex( statement, labelId, query );
    }

    private ResourceIterator<Node> nodesByLabelAndProperties(
            KernelTransaction transaction, int labelId, IndexQuery.ExactPredicate... queries )
    {
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();

        if ( isInvalidQuery( labelId, queries ) )
        {
            statement.close();
            return emptyResourceIterator();
        }

        int[] propertyIds = getPropertyIds( queries );
        IndexDescriptor index = findMatchingIndex( transaction, labelId, propertyIds );

        if ( index != IndexDescriptor.NO_INDEX )
        {
            try
            {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                IndexReadSession indexSession = read.indexReadSession( index );
                read.nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, getReorderedIndexQueries( index.schema().getPropertyIds(), queries ) );
                return new NodeCursorResourceIterator<>( cursor, statement, this::newNodeProxy );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a label scan
            }
        }
        return getNodesByLabelAndPropertyWithoutIndex( statement, labelId, queries );
    }

    private static IndexDescriptor findMatchingIndex( KernelTransaction transaction, int labelId, int[] propertyIds )
    {
        IndexDescriptor index = transaction.schemaRead().index( labelId, propertyIds );
        if ( index != IndexDescriptor.NO_INDEX )
        {
            // index found with property order matching the query
            return index;
        }
        else
        {
            // attempt to find matching index with different property order
            Arrays.sort( propertyIds );
            assertNoDuplicates( propertyIds, transaction.tokenRead() );

            int[] workingCopy = new int[propertyIds.length];

            Iterator<IndexDescriptor> indexes = transaction.schemaRead().indexesGetForLabel( labelId );
            while ( indexes.hasNext() )
            {
                index = indexes.next();
                int[] original = index.schema().getPropertyIds();
                if ( hasSamePropertyIds( original, workingCopy, propertyIds ) )
                {
                    // Ha! We found an index with the same properties in another order
                    return index;
                }
            }
            return IndexDescriptor.NO_INDEX;
        }
    }

    private static IndexQuery[] getReorderedIndexQueries( int[] indexPropertyIds, IndexQuery[] queries )
    {
        IndexQuery[] orderedQueries = new IndexQuery[queries.length];
        for ( int i = 0; i < indexPropertyIds.length; i++ )
        {
            int propertyKeyId = indexPropertyIds[i];
            for ( IndexQuery query : queries )
            {
                if ( query.propertyKeyId() == propertyKeyId )
                {
                    orderedQueries[i] = query;
                    break;
                }
            }
        }
        return orderedQueries;
    }

    private static boolean hasSamePropertyIds( int[] original, int[] workingCopy, int[] propertyIds )
    {
        if ( original.length == propertyIds.length )
        {
            System.arraycopy( original, 0, workingCopy, 0, original.length );
            Arrays.sort( workingCopy );
            return Arrays.equals( propertyIds, workingCopy );
        }
        return false;
    }

    private static int[] getPropertyIds( IndexQuery[] queries )
    {
        int[] propertyIds = new int[queries.length];
        for ( int i = 0; i < queries.length; i++ )
        {
            propertyIds[i] = queries[i].propertyKeyId();
        }
        return propertyIds;
    }

    private static boolean isInvalidQuery( int labelId, IndexQuery[] queries )
    {
        boolean invalidQuery = labelId == TokenRead.NO_TOKEN;
        for ( IndexQuery query : queries )
        {
            int propertyKeyId = query.propertyKeyId();
            invalidQuery = invalidQuery || propertyKeyId == TokenRead.NO_TOKEN;
        }
        return invalidQuery;
    }

    private static void assertNoDuplicates( int[] propertyIds, TokenRead tokenRead )
    {
        int prev = propertyIds[0];
        for ( int i = 1; i < propertyIds.length; i++ )
        {
            int curr = propertyIds[i];
            if ( curr == prev )
            {
                SilentTokenNameLookup tokenLookup = new SilentTokenNameLookup( tokenRead );
                throw new IllegalArgumentException(
                        format( "Provided two queries for property %s. Only one query per property key can be performed",
                                tokenLookup.propertyKeyGetName( curr ) ) );
            }
            prev = curr;
        }
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex(
            Statement statement, int labelId, IndexQuery... queries )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor();
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor();
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor();

        transaction.dataRead().nodeLabelScan( labelId, nodeLabelCursor );

        return new NodeLabelPropertyIterator( transaction.dataRead(),
                                                nodeLabelCursor,
                                                nodeCursor,
                                                propertyCursor,
                                                statement,
                                                this::newNodeProxy,
                                                queries );
    }

    private ResourceIterator<Node> allNodesWithLabel( final Label myLabel )
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        Statement statement = ktx.acquireStatement();

        int labelId = ktx.tokenRead().nodeLabel( myLabel.name() );
        if ( labelId == TokenRead.NO_TOKEN )
        {
            statement.close();
            return Iterators.emptyResourceIterator();
        }

        NodeLabelIndexCursor cursor = ktx.cursors().allocateNodeLabelIndexCursor();
        ktx.dataRead().nodeLabelScan( labelId, cursor );
        return new NodeCursorResourceIterator<>( cursor, statement, this::newNodeProxy );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( () -> statementContext.get( databaseId() ) );
    }

    @Override
    public String databaseName()
    {
        return databaseId().name();
    }

    // GraphDatabaseAPI
    @Override
    public DependencyResolver getDependencyResolver()
    {
        return database.getDependencyResolver();
    }

    @Override
    public StoreId storeId()
    {
        return database.getStoreId();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    @Override
    public String toString()
    {
        return databaseInfo + " [" + databaseLayout() + "]";
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
    }

    @Override
    public void assertInUnterminatedTransaction()
    {
        statementContext.assertInUnterminatedTransaction();
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id )
    {
        return new RelationshipProxy( this, id );
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipProxy( this, id, startNodeId, typeId, endNodeId );
    }

    @Override
    public NodeProxy newNodeProxy( long nodeId )
    {
        return new NodeProxy( this, nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        try
        {
            String name = tokenHolders.relationshipTypeTokens().getTokenById( type ).name();
            return RelationshipType.withName( name );
        }
        catch ( TokenNotFoundException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + type );
        }
    }

    private static class NodeLabelPropertyIterator extends PrefetchingNodeResourceIterator
    {
        private final Read read;
        private final NodeLabelIndexCursor nodeLabelCursor;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        private final IndexQuery[] queries;

        NodeLabelPropertyIterator(
                Read read,
                NodeLabelIndexCursor nodeLabelCursor,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                Statement statement,
                NodeFactory nodeFactory,
                IndexQuery... queries )
        {
            super( statement, nodeFactory );
            this.read = read;
            this.nodeLabelCursor = nodeLabelCursor;
            this.nodeCursor = nodeCursor;
            this.propertyCursor = propertyCursor;
            this.queries = queries;
        }

        @Override
        protected long fetchNext()
        {
            boolean hasNext;
            do
            {
                hasNext = nodeLabelCursor.next();

            } while ( hasNext && !hasPropertiesWithValues() );

            if ( hasNext )
            {
                return nodeLabelCursor.nodeReference();
            }
            else
            {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources( Statement statement )
        {
            IOUtils.closeAllSilently( statement, nodeLabelCursor, nodeCursor, propertyCursor );
        }

        private boolean hasPropertiesWithValues()
        {
            int targetCount = queries.length;
            read.singleNode( nodeLabelCursor.nodeReference(), nodeCursor );
            if ( nodeCursor.next() )
            {
                nodeCursor.properties( propertyCursor );
                while ( propertyCursor.next() )
                {
                    for ( IndexQuery query : queries )
                    {
                        if ( propertyCursor.propertyKey() == query.propertyKeyId() )
                        {
                            if ( query.acceptsValueAt( propertyCursor ) )
                            {
                                targetCount--;
                                if ( targetCount == 0 )
                                {
                                    return true;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    private void assertTransactionOpen()
    {
        assertTransactionOpen( statementContext.getKernelTransactionBoundToThisThread( true, databaseId() ) );
    }

    private static void assertTransactionOpen( KernelTransaction transaction )
    {
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
    }

    private static final class NodeCursorResourceIterator<CURSOR extends NodeIndexCursor> extends PrefetchingNodeResourceIterator
    {
        private final CURSOR cursor;

        NodeCursorResourceIterator( CURSOR cursor, Statement statement, NodeFactory nodeFactory )
        {
            super( statement, nodeFactory );
            this.cursor = cursor;
        }

        @Override
        long fetchNext()
        {
            if ( cursor.next() )
            {
                return cursor.nodeReference();
            }
            else
            {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources( Statement statement )
        {
            IOUtils.closeAllSilently( statement, cursor );
        }
    }

    private abstract static class PrefetchingNodeResourceIterator implements ResourceIterator<Node>
    {
        private final Statement statement;
        private final NodeFactory nodeFactory;
        private long next;
        private boolean closed;

        private static final long NOT_INITIALIZED = -2L;
        protected static final long NO_ID = -1L;

        PrefetchingNodeResourceIterator( Statement statement, NodeFactory nodeFactory )
        {
            this.statement = statement;
            this.nodeFactory = nodeFactory;
            this.next = NOT_INITIALIZED;
        }

        @Override
        public boolean hasNext()
        {
            if ( next == NOT_INITIALIZED )
            {
                next = fetchNext();
            }
            return next != NO_ID;
        }

        @Override
        public Node next()
        {
            if ( !hasNext() )
            {
                close();
                throw new NoSuchElementException(  );
            }
            Node nodeProxy = nodeFactory.make( next );
            next = fetchNext();
            return nodeProxy;
        }

        @Override
        public void close()
        {
            if ( !closed )
            {
                next = NO_ID;
                closeResources( statement );
                closed = true;
            }
        }

        abstract long fetchNext();

        abstract void closeResources( Statement statement );
    }

    private interface NodeFactory
    {
        NodeProxy make( long id );
    }
}
