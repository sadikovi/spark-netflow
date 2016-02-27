/*
 * Copyright 2016 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sadikovi.netflowlib.predicate;

import java.util.HashMap;

import com.github.sadikovi.netflowlib.predicate.Operators.FilterPredicate;
import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.Ge;
import com.github.sadikovi.netflowlib.predicate.Operators.Lt;
import com.github.sadikovi.netflowlib.predicate.Operators.Le;
import com.github.sadikovi.netflowlib.predicate.Operators.In;
import com.github.sadikovi.netflowlib.predicate.Operators.And;
import com.github.sadikovi.netflowlib.predicate.Operators.Or;
import com.github.sadikovi.netflowlib.predicate.Operators.Not;
import com.github.sadikovi.netflowlib.predicate.Operators.TrivialPredicate;
import com.github.sadikovi.netflowlib.statistics.Statistics;

/**
 * Abstract [[PredicateTransform]] interface allows to modify predicate tree. By default predicate
 * tree is immutable, it is recommended to always return a new node, if modified. Also some
 * optimizations might apply in concrete implementations.
 */
public abstract interface PredicateTransform {
  <T extends Comparable<T>> FilterPredicate transform(Eq<T> predicate,
    HashMap<String, Statistics> stats);

  <T extends Comparable<T>> FilterPredicate transform(Gt<T> predicate,
    HashMap<String, Statistics> stats);

  <T extends Comparable<T>> FilterPredicate transform(Ge<T> predicate,
    HashMap<String, Statistics> stats);

  <T extends Comparable<T>> FilterPredicate transform(Lt<T> predicate,
    HashMap<String, Statistics> stats);

  <T extends Comparable<T>> FilterPredicate transform(Le<T> predicate,
    HashMap<String, Statistics> stats);

  <T extends Comparable<T>> FilterPredicate transform(In<T> predicate,
    HashMap<String, Statistics> stats);

  FilterPredicate transform(And predicate);

  FilterPredicate transform(Or predicate);

  FilterPredicate transform(Not predicate);
}
