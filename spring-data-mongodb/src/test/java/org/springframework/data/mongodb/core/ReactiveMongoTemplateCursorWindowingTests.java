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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.domain.CursorRequest;
import org.springframework.data.domain.CursorWindow;
import org.springframework.data.domain.KeysetCursorRequest;
import org.springframework.data.domain.OffsetCursorRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link CursorWindow} queries.
 *
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
class ReactiveMongoTemplateCursorWindowingTests {

	static @Client MongoClient client;

	public static final String DB_NAME = "mongo-template-cursor-windowing-tests";

	ConfigurableApplicationContext context = new GenericApplicationContext();

	private ReactiveMongoTestTemplate template = new ReactiveMongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureApplicationContext(it -> {
			it.applicationContext(context);
		});
	});

	@BeforeEach
	void setUp() {
		template.remove(Person.class).all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@ParameterizedTest // GH-4308
	@MethodSource("cursors")
	public <T> void shouldApplyCursoringCorrectly(CursorRequest cursorRequest, Class<T> resultType,
			Function<Person, T> assertionConverter) {

		Person john20 = new Person("John", 20);
		Person john40_1 = new Person("John", 40);
		Person john40_2 = new Person("John", 40);
		Person jane_20 = new Person("Jane", 20);
		Person jane_40 = new Person("Jane", 40);
		Person jane_42 = new Person("Jane", 42);

		template.insertAll(Arrays.asList(john20, john40_1, john40_2, jane_20, jane_40, jane_42)) //
				.as(StepVerifier::create) //
				.expectNextCount(6) //
				.verifyComplete();

		Query q = new Query(where("firstName").regex("J.*"));

		CursorWindow<T> window = template.findWindow(cursorRequest, q, resultType, "person").block(Duration.ofSeconds(10));

		assertThat(window.isFirst()).isTrue();
		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(assertionConverter.apply(jane_20), assertionConverter.apply(jane_40));

		window = template.findWindow(window.nextCursorRequest().withSize(3), q, resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(window.isFirst()).isFalse();
		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(3);
		assertThat(window).contains(assertionConverter.apply(jane_42), assertionConverter.apply(john20));
		assertThat(window).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));

		window = template.findWindow(window.nextCursorRequest().withSize(1), q, resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(window.isFirst()).isFalse();
		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));
	}

	public static Stream<Arguments> cursors() {

		Sort sort = Sort.by("firstName", "age");

		return Stream.of(args(KeysetCursorRequest.ofSize(2, sort), Person.class, Function.identity()), //
				args(KeysetCursorRequest.ofSize(2, sort), Document.class,
						ReactiveMongoTemplateCursorWindowingTests::toDocument), //
				args(OffsetCursorRequest.ofSize(2, sort), Person.class, Function.identity()));
	}

	private static <T> Arguments args(CursorRequest cursorRequest, Class<T> resultType,
			Function<Person, T> assertionConverter) {
		return Arguments.of(cursorRequest, resultType, assertionConverter);
	}

	static Document toDocument(Person person) {
		return new Document("_class", person.getClass().getName()).append("_id", person.getId()).append("active", true)
				.append("firstName", person.getFirstName()).append("age", person.getAge());
	}
}
