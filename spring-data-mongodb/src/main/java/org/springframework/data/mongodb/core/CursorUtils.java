/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.data.domain.CursorRequest;
import org.springframework.data.domain.CursorWindow;
import org.springframework.data.domain.KeysetCursorRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.EntityOperations.Entity;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Utilities to run cursor window queries and create cursor window results.
 *
 * @author Mark Paluch
 * @since 4.1
 */
class CursorUtils {

	static boolean hasMoreElements(CursorRequest request, List<?> result) {
		return !result.isEmpty() && result.size() > request.getSize();
	}

	static boolean isLast(CursorRequest request, List<?> result) {
		return !hasMoreElements(request, result);
	}

	static <T> List<T> getSubList(CursorRequest request, List<T> result) {

		if (result.size() > request.getSize()) {
			return result.subList(0, request.getSize());
		}

		return result;
	}

	/**
	 * Create the actual query to run keyset-based pagination. Affects projection, sorting, and the criteria.
	 *
	 * @param query
	 * @param cursorRequest
	 * @param idPropertyName
	 * @return
	 */
	static KeySetCursorQuery createKeysetPaginationQuery(Query query, KeysetCursorRequest cursorRequest,
			String idPropertyName) {

		Sort targetSort = cursorRequest.getSort().and(Sort.by(Order.asc(idPropertyName)));

		// make sure we can extract the keyset
		Document fieldsObject = query.getFieldsObject();
		if (!fieldsObject.isEmpty()) {
			fieldsObject.put(idPropertyName, 1);
			for (Order order : cursorRequest.getSort()) {
				fieldsObject.put(order.getProperty(), 1);
			}
		}

		Document sortObject = query.isSorted() ? query.getSortObject() : new Document();
		targetSort.forEach(order -> sortObject.put(order.getProperty(), order.isAscending() ? 1 : -1));
		sortObject.put(idPropertyName, 1);

		Document queryObject = query.getQueryObject();

		List<Document> or = (List<Document>) queryObject.getOrDefault("$or", new ArrayList<>());

		Map<String, Object> keysetValues = cursorRequest.getKeys();
		Document keysetSort = new Document();
		List<String> sortKeys = new ArrayList<>(sortObject.keySet());

		if (!keysetValues.isEmpty() && !keysetValues.keySet().containsAll(sortKeys)) {
			throw new IllegalStateException("KeysetCursorRequest does not contain all keyset values");
		}

		// first query doesn't come with a keyset
		if (!keysetValues.isEmpty()) {

			// build matrix query for keyset paging that contains sort^2 queries
			// reflecting a query that follows sort order semantics starting from the last returned keyset
			for (int i = 0; i < sortKeys.size(); i++) {

				Document sortConstraint = new Document();

				for (int j = 0; j < sortKeys.size(); j++) {

					String sortSegment = sortKeys.get(j);
					int sortOrder = sortObject.getInteger(sortSegment);
					Object o = keysetValues.get(sortSegment);

					if (j >= i) { // tail segment
						sortConstraint.put(sortSegment, new Document(sortOrder == 1 ? "$gt" : "$lt", o));
						break;
					}

					sortConstraint.put(sortSegment, o);
				}

				if (!sortConstraint.isEmpty()) {
					or.add(sortConstraint);
				}
			}
		}

		if (!keysetSort.isEmpty()) {
			or.add(keysetSort);
		}
		if (!or.isEmpty()) {
			queryObject.put("$or", or);
		}

		return new KeySetCursorQuery(queryObject, fieldsObject, sortObject);
	}

	static <T> CursorWindow<T> createWindow(KeysetCursorRequest cursorRequest, List<T> result,
			EntityOperations operations) {

		if (CursorUtils.hasMoreElements(cursorRequest, result)) {

			T last = result.get(cursorRequest.getSize() - 1);
			Entity<T> entity = operations.forEntity(last);

			Map<String, Object> keys = entity.extractKeys(cursorRequest.getSort());
			cursorRequest = cursorRequest.withNext(keys);
		}

		return createWindow(cursorRequest, result);
	}

	static <T> CursorWindow<T> createWindow(CursorRequest cursorRequest, List<T> result) {
		return CursorWindow.from(cursorRequest.withLast(isLast(cursorRequest, result)), getSubList(cursorRequest, result));
	}

	record KeySetCursorQuery(Document query, Document fields, Document sort) {

	}

}
