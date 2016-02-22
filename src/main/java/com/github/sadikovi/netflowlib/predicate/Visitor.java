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

import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.Ge;
import com.github.sadikovi.netflowlib.predicate.Operators.Lt;
import com.github.sadikovi.netflowlib.predicate.Operators.Le;
import com.github.sadikovi.netflowlib.predicate.Operators.In;

/**
 * Abstract [[Visitor]] interface describes how one of the leaf predicates is parsed. Visitor
 * should provide all the neccessary information to resolve passed predicate. Either
 * [[PredicateTransform]] or [[Visitor]] interfaces should be used, but not both of them.
 */
public abstract interface Visitor {
  <T extends Comparable<T>> boolean accept(Eq<T> predicate);

  <T extends Comparable<T>> boolean accept(Gt<T> predicate);

  <T extends Comparable<T>> boolean accept(Ge<T> predicate);

  <T extends Comparable<T>> boolean accept(Lt<T> predicate);

  <T extends Comparable<T>> boolean accept(Le<T> predicate);

  <T extends Comparable<T>> boolean accept(In<T> predicate);
}
