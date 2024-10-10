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
package org.neo4j.server.queryapi;

import static org.neo4j.server.queryapi.request.AccessMode.toDriverAccessMode;
import static org.neo4j.server.queryapi.response.HttpErrorResponse.fromDriverException;
import static org.neo4j.server.queryapi.response.HttpErrorResponse.singleError;
import static org.neo4j.server.queryapi.response.TypedJsonDriverResultWriter.TYPED_JSON_MIME_TYPE_VALUE;

import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsMonitor;
import org.neo4j.server.queryapi.request.AccessMode;
import org.neo4j.server.queryapi.request.QueryRequest;
import org.neo4j.server.queryapi.request.ResultContainer;
import org.neo4j.server.queryapi.response.HttpErrorResponse;
import org.neo4j.server.rest.dbms.AuthorizationHeaders;

@Path(QueryResource.FULL_PATH)
public class QueryResource {

    public static final String NAME = "query";
    private static final String DB_PATH_PARAM_NAME = "databaseName";
    public static final String API_PATH_FRAGMENT = "query/v2";
    static final String FULL_PATH = "/{" + DB_PATH_PARAM_NAME + "}/" + API_PATH_FRAGMENT;

    private final Driver driver;
    private final InternalLog log;
    private final QueryAPIMetricsMonitor monitor;

    public QueryResource(@Context Driver driver, @Context InternalLog log, @Context QueryAPIMetricsMonitor monitor) {
        this.driver = driver;
        this.log = log;
        this.monitor = monitor;
    }

    @POST
    @Produces({"application/json", TYPED_JSON_MIME_TYPE_VALUE})
    @Consumes({"application/json", TYPED_JSON_MIME_TYPE_VALUE})
    public Response execute(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        meterRequest(request);
        var sessionConfig = buildSessionConfig(request, databaseName);
        // The session will be closed after the result set has been serialized, it must not be closed in a
        // try-with-resources block here. It must be closed only in an exceptional state
        var sessionAuthToken = extractAuthToken(rawRequest);
        if (sessionAuthToken == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Session session = driver.session(Session.class, sessionConfig, sessionAuthToken);

        Response response;
        try {
            var result = session.run(request.statement(), request.parameters());
            var resultAndSession = new ResultContainer(result, session, request);
            response = Response.accepted(resultAndSession).build();
        } catch (FatalDiscoveryException ex) {
            response = generateResponse(Response.Status.NOT_FOUND, fromDriverException(ex));
        } catch (ClientException | TransientException clientException) {
            response = generateResponse(Response.Status.BAD_REQUEST, fromDriverException(clientException));
        } catch (Neo4jException neo4jException) {
            response = generateResponse(Response.Status.INTERNAL_SERVER_ERROR, fromDriverException(neo4jException));
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            response = generateResponse(
                    Response.Status.INTERNAL_SERVER_ERROR,
                    singleError(
                            Status.General.UnknownError.code().serialize(),
                            Status.General.UnknownError.code().description()));
        }

        if (response.getStatus() != Response.Status.ACCEPTED.getStatusCode()) {
            closeSession(session);
        }

        return response;
    }

    private void meterRequest(QueryRequest request) {
        if (request.accessMode() != null && request.accessMode().equals(AccessMode.READ)) {
            monitor.readRequest();
        }
        if (request.parameters() != null && !request.parameters().isEmpty()) {
            monitor.parameter();
        }
    }

    private void closeSession(Session session) {
        if (session != null) {
            session.close();
        }
    }

    private SessionConfig buildSessionConfig(QueryRequest request, String databaseName) {
        var sessionConfigBuilder = SessionConfig.builder().withDatabase(databaseName);

        if (!(request.bookmarks() == null || request.bookmarks().isEmpty())) {
            sessionConfigBuilder.withBookmarks(
                    request.bookmarks().stream().map(Bookmark::from).collect(Collectors.toList()));
        }

        if (!(request.impersonatedUser() == null || request.impersonatedUser().isBlank())) {
            sessionConfigBuilder.withImpersonatedUser(request.impersonatedUser().trim());
        }

        if (request.accessMode() != null) {
            sessionConfigBuilder.withDefaultAccessMode(toDriverAccessMode(request.accessMode()));
        }

        return sessionConfigBuilder.build();
    }

    private static AuthToken extractAuthToken(HttpServletRequest request) {
        // Auth has already passed through AuthorizationEnabledFilter, so we know we have formatted credential
        var authHeader = request.getHeader("Authorization");

        if (authHeader == null) {
            return AuthTokens.none();
        }

        var decoded = AuthorizationHeaders.decode(authHeader);
        if (decoded == null) {
            return AuthTokens.none();
        }

        return switch (decoded.scheme()) {
            case BEARER -> AuthTokens.bearer(decoded.values()[0]);
            case BASIC -> AuthTokens.basic(decoded.values()[0], decoded.values()[1]);
            default -> AuthTokens.none();
        };
    }

    public static String absoluteDatabaseTransactionPath(Config config) {
        return config.get(ServerSettings.db_api_path).getPath() + FULL_PATH;
    }

    private Response generateResponse(Response.Status status, HttpErrorResponse message) {
        return Response.status(status).entity(message).build();
    }
}
