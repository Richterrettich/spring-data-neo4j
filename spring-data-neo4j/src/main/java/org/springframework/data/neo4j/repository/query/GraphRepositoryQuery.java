/*
 * Copyright (c)  [2011-2015] "Pivotal Software, Inc." / "Neo Technology" / "Graph Aware Ltd."
 *
 * This product is licensed to you under the Apache License, Version 2.0 (the "License").
 * You may not use this product except in compliance with the License.
 *
 * This product may include a number of subcomponents with
 * separate copyright notices and license terms. Your use of the source
 * code for these subcomponents is subject to the terms and
 * conditions of the subcomponent's license, as noted in the LICENSE file.
 */

package org.springframework.data.neo4j.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.ogm.cypher.query.Pagination;
import org.neo4j.ogm.session.Session;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.neo4j.util.IterableUtils;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.RepositoryQuery;


/**
 * Specialisation of {@link RepositoryQuery} that handles mapping to object annotated with <code>&#064;Query</code>.
 *
 * @author Mark Angrish
 * @author Luanne Misquitta
 */
public class GraphRepositoryQuery implements RepositoryQuery {

    private final GraphQueryMethod graphQueryMethod;

    protected final Session session;

    public GraphRepositoryQuery(GraphQueryMethod graphQueryMethod, Session session) {
        this.graphQueryMethod = graphQueryMethod;
        this.session = session;
    }

    @Override
    public final Object execute(Object[] parameters) {
        Class<?> returnType = graphQueryMethod.getMethod().getReturnType();
        Class<?> concreteType = graphQueryMethod.resolveConcreteReturnType();

        graphQueryMethod.isPageQuery();
        Map<String, Object> params = resolveParams(parameters);
        if(graphQueryMethod.getParameters().hasPageableParameter()) {
            Pageable pageable = (Pageable)parameters[graphQueryMethod.getParameters().getPageableIndex()];
            return execute(returnType, concreteType, getQueryString(), params,pageable);
        } else if (graphQueryMethod.getParameters().hasSortParameter()) {
            Sort sort = (Sort) parameters[graphQueryMethod.getParameters().getSortIndex()];
            return execute(returnType, concreteType, getQueryString(), params,sort);
        }
        return execute(returnType, concreteType, getQueryString(), params);
    }

    protected Object execute(Class<?> returnType, Class<?> concreteType, String cypherQuery, Map<String, Object> queryParams,Sort sort) {
        return session.query(concreteType, cypherQuery, queryParams,PagingAndSortingUtils.convertSort(sort));
    }

    protected Object execute(Class<?> returnType, Class<?> concreteType, String cypherQuery, Map<String, Object> queryParams,Pageable pageable) {
        Pagination pagination = new Pagination(pageable.getPageNumber(),pageable.getPageSize());
        Iterable result = session.query(concreteType, cypherQuery, queryParams,PagingAndSortingUtils.convertSort(pageable.getSort()),pagination);
        if(graphQueryMethod.isPageQuery()) {
            result = PagingAndSortingUtils.updatePage(pageable,IterableUtils.toList(result));
        }
        return result;
    }

    protected Object execute(Class<?> returnType, Class<?> concreteType, String cypherQuery, Map<String, Object> queryParams) {
        if (returnType.equals(Void.class)) {
            session.execute(cypherQuery, queryParams);
            return null;
        }
        if (Iterable.class.isAssignableFrom(returnType)) {
            // Special method to handle SDN Iterable<Map<String, Object>> behaviour.
            // TODO: Do we really want this method in an OGM? It's a little too low level and/or doesn't really fit.
            if (Map.class.isAssignableFrom(concreteType)) {
                return session.query(cypherQuery, queryParams);
            }

            return session.query(concreteType, cypherQuery, queryParams);
        }

        return session.queryForObject(returnType, cypherQuery, queryParams);
    }

    private Map<String, Object> resolveParams(Object[] parameters) {
        Map<String, Object> params = new HashMap<>();
        Parameters<?, ?> methodParameters = graphQueryMethod.getParameters();

        for (int i = 0; i < parameters.length; i++) {
            if(parameters[i] instanceof Pageable || parameters [i] instanceof Sort) {
                continue;
            }
            Parameter parameter = methodParameters.getParameter(i);

            if (parameter.isNamedParameter()) {
                params.put(parameter.getName(), parameters[i]);
            } else {
                params.put("" + i, parameters[i]);
            }
        }
        return params;
    }

    @Override
    public GraphQueryMethod getQueryMethod() {
        return graphQueryMethod;
    }

    protected String getQueryString() {
        return getQueryMethod().getQuery();
    }

}