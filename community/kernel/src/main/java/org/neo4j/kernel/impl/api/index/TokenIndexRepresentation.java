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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexSample;

/**
 * Method {@link #changeIndexDescriptor} allows changing the descriptor.
 * However this is a very special operation and should be used with care.
 * <p>
 * Also collecting statistics on token indexes is not needed and therefore not supported.
 */
class TokenIndexRepresentation implements IndexRepresentation
{
    private volatile IndexDescriptor descriptor;
    private final TokenNameLookup tokenNameLookup;
    private final boolean allowToChangeDescriptor;

    TokenIndexRepresentation( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup, boolean allowToChangeDescriptor )
    {
        this.descriptor = descriptor;
        this.tokenNameLookup = tokenNameLookup;
        this.allowToChangeDescriptor = allowToChangeDescriptor;
    }

    @Override
    public IndexDescriptor getIndexDescriptor()
    {
        return descriptor;
    }

    @Override
    public void removeStatisticsForIndex()
    {

    }

    @Override
    public void incrementUpdateStatisticsForIndex( long delta )
    {

    }

    @Override
    public void replaceStatisticsForIndex( IndexSample sample )
    {

    }

    @Override
    public void changeIndexDescriptor( IndexDescriptor descriptor )
    {
        if ( !allowToChangeDescriptor )
        {
            throw new UnsupportedOperationException( "Changing descriptor on this index representation is not allowed" );
        }

        this.descriptor = descriptor;
    }

    @Override
    public String getIndexUserDescription()
    {
        return descriptor.userDescription( tokenNameLookup );
    }
}
