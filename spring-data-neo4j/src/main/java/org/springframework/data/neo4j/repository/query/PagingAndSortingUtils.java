/*
 * Copyright (c)  [2011-2015] "Pivotal Software, Inc." / "Neo Technology" / "Graph Aware Ltd."
 *
 * This product is licensed to you under the Apache License, Version 2.0 (the "License").
 * You may not use this product except in compliance with the License.
 *
 * This product may include a number of subcomponents with
 * separate copyright notices and license terms. Your use of the source
 *  code for these subcomponents is subject to the terms and
 *  conditions of the subcomponent's license, as noted in the LICENSE file.
 *
 *
 */

package org.springframework.data.neo4j.repository.query;

import org.neo4j.ogm.cypher.query.SortOrder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;

/**
 * @author Rene Richter
 */
public class PagingAndSortingUtils {

    public static SortOrder convertSort(Sort sort) {
        SortOrder sortOrder = new SortOrder();
        if (sort != null) {
            for (Sort.Order order : sort) {
                if (order.isAscending()) {
                    sortOrder.add(order.getProperty());
                } else {
                    sortOrder.add(SortOrder.Direction.DESC, order.getProperty());
                }
            }
        }
        return sortOrder;
    }

    /*
     * This is a cheap trick to estimate the total number of objects without actually knowing the real value.
     * Essentially, if the result size is the same as the page size, we assume more data can be fetched, so
     * we set the expected total to the current total retrieved so far + the current page size. As soon as the
     * result size is less than the page size, we know there are no more, so we set the total to the number
     * retrieved so far. This will ensure that page.next() returns false.
     */
    public static <T>Page<T> updatePage(Pageable pageable, List<T> results) {
        int pageSize = pageable.getPageSize();
        int pageOffset = pageable.getOffset();
        int total = pageOffset + results.size() + (results.size() == pageSize ? pageSize : 0);

        return new PageImpl<T>(results, pageable, total);
    }
}
