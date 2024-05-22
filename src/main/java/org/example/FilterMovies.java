/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterMovies {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("/Users/inistar/IdeaProjects/movieanalysis/src/main/resources/ml-latest-small/movies.csv")
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class);

		DataSet<Movie> movies = lines.map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
			@Override
			public Movie map(Tuple3<Long, String, String> csvLine) throws Exception {
				String movieName = csvLine.f1;
				String[] genres = csvLine.f2.split("\\|");
				return new Movie(movieName, new HashSet<>(Arrays.asList(genres)));
			}
		});

		DataSet<Movie> filteredMovies = movies.filter(new FilterFunction<Movie>() {
			@Override
			public boolean filter(Movie movie) throws Exception {
				return movie.getGenres().contains("Drama");
			}
		});

		filteredMovies.writeAsText("filter-output");
		env.execute();
	}
}

class Movie{
	private String name;
	private Set<String> genres;

	public Movie(String name, Set<String> genres){
		this.name = name;
		this.genres = genres;
	}

	public String getName() { return name; }

	public Set<String> getGenres() { return genres; }

	@Override
	public String toString(){
		return "Movie{" +
				"name=" + name + '\'' +
				", genres=" + genres + '}';
	}

}