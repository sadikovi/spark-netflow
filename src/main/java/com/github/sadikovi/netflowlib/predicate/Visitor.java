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

import com.github.sadikovi.netflowlib.predicate.Inspectors.AndInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.NotInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.OrInspector;
import com.github.sadikovi.netflowlib.predicate.Inspectors.ValueInspector;

/**
 * [[Visitor]] interface to traverse [[Inspector]] instances and resolve boolean expressions for
 * each of them. Usually implemented by [[RecordMaterializer]] subclasses to resolve predicate
 * for each record.
 */
public abstract interface Visitor {
  boolean visit(ValueInspector inspector);

  boolean visit(AndInspector inspector);

  boolean visit(OrInspector inspector);

  boolean visit(NotInspector inspector);
}
