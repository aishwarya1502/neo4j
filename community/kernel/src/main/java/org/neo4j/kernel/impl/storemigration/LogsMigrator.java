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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.DatabaseNotCleanlyShutDownException;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;

// TODO: this class needs some love when the migration and upgrade code paths are split.
public class LogsMigrator
{
    private static final String UPGRADE_CHECKPOINT = "Upgrade checkpoint.";
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactoryToMigrateFrom;
    private final StorageEngineFactory storageEngineFactoryToMigrateTo;
    private final DatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final Config config;
    private final CursorContextFactory contextFactory;
    private final Supplier<LogTailMetadata> logTailSupplier;

    public LogsMigrator(
            FileSystemAbstraction fs,
            StorageEngineFactory storageEngineFactoryToMigrateFrom,
            StorageEngineFactory storageEngineFactoryToMigrateTo,
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            Config config,
            CursorContextFactory contextFactory,
            Supplier<LogTailMetadata> logTailSupplier )
    {
        this.fs = fs;
        this.storageEngineFactoryToMigrateFrom = storageEngineFactoryToMigrateFrom;
        this.storageEngineFactoryToMigrateTo = storageEngineFactoryToMigrateTo;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.config = config;
        this.contextFactory = contextFactory;
        this.logTailSupplier = logTailSupplier;
    }

    public void assertCleanlyShutDown()
    {
        Throwable suppressibleException = null;
        var logTail = logTailSupplier.get();
        try
        {
            if ( !logTail.isRecoveryRequired() )
            {
                // All good
                return;
            }
            if ( logTail.logsMissing() && !config.get( fail_on_missing_files ) )
            {
                // We don't have any log files, but we were told to ignore this.
                return;
            }
        }
        catch ( Throwable throwable )
        {
            // ignore exception and throw db not cleanly shutdown
            suppressibleException = throwable;
        }
        DatabaseNotCleanlyShutDownException exception = upgradeException( logTail );
        if ( suppressibleException != null )
        {
            exception.addSuppressed( suppressibleException );
        }
        throw exception;
    }

    public void upgrade( DatabaseLayout layout ) throws IOException
    {
        try ( MetadataProvider store = getMetaDataStore() )
        {
            TransactionLogInitializer logInitializer = new TransactionLogInitializer( fs, store, storageEngineFactoryToMigrateTo );

            Path transactionLogsDirectory = layout.getTransactionLogsDirectory();
            LogFiles logFiles =
                    LogFilesBuilder.logFilesBasedOnlyBuilder( transactionLogsDirectory, fs ).withExternalLogTailMetadata( logTailSupplier.get() ).build();
            var files = logFiles.logFiles();
            if ( files != null && files.length > 0 )
            {
                logInitializer.upgradeExistingLogFiles( layout, transactionLogsDirectory, UPGRADE_CHECKPOINT );
            }
            else if ( config.get( fail_on_missing_files ) )
            {
                // The log files are missing entirely.
                // By default, we should avoid modifying stores that have no log files,
                // since the log files are the only thing that can tell us if the store is in a
                // recovered state or not.
                throw new UpgradeNotAllowedException();
            }
            else
            {
                // The log files are missing entirely, but we were told to not think of this as an error condition,
                // so we instead initialize an empty log file.
                logInitializer.initializeEmptyLogFile( layout, transactionLogsDirectory, UPGRADE_CHECKPOINT );
            }
        }
        catch ( Exception exception )
        {
            throw new StoreUpgrader.TransactionLogsUpgradeException( "Failure on attempt to upgrade transaction logs to new version.", exception );
        }
    }

    private MetadataProvider getMetaDataStore() throws IOException
    {
        return storageEngineFactoryToMigrateTo.transactionMetaDataStore( fs, databaseLayout, config, pageCache, DatabaseReadOnlyChecker.readOnly(),
                contextFactory, logTailSupplier.get() );
    }

    private static DatabaseNotCleanlyShutDownException upgradeException( LogTailMetadata tail )
    {
        return tail == null ? new DatabaseNotCleanlyShutDownException() : new DatabaseNotCleanlyShutDownException( tail );
    }
}
