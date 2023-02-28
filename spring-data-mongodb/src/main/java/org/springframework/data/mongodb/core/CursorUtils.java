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

import java.util.List;

import org.springframework.data.domain.CursorRequest;
import org.springframework.data.domain.CursorWindow;

/**
 * @author Mark Paluch
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

	public static <T> CursorWindow<T> createWindow(CursorRequest cursorRequest, List<T> result) {
		return CursorWindow.from(cursorRequest.withLast(isLast(cursorRequest, result)), getSubList(cursorRequest, result));
	}
}
