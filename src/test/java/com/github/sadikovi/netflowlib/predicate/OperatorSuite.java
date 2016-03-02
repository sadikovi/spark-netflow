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

import java.util.HashSet;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.sadikovi.netflowlib.predicate.Columns.IntColumn;
import com.github.sadikovi.netflowlib.predicate.Columns.ShortColumn;
import com.github.sadikovi.netflowlib.predicate.FilterApi;
import com.github.sadikovi.netflowlib.predicate.Operators.Eq;
import com.github.sadikovi.netflowlib.predicate.Operators.Gt;
import com.github.sadikovi.netflowlib.predicate.Operators.In;
import com.github.sadikovi.netflowlib.predicate.Operators.And;

public class OperatorSuite {
  @Test
  public void testEquality() {
    IntColumn col = new IntColumn("col1", 0);

    Eq pr1 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr1));

    Eq pr2 = FilterApi.eq(col, 10);
    assertTrue(pr1.equals(pr2));

    Gt pr3 = FilterApi.gt(col, 10);
    assertFalse(pr1.equals(pr3));

    HashSet<Integer> set = new HashSet<Integer>();
    set.add(10);
    In pr4 = FilterApi.in(col, set);
    assertFalse(pr1.equals(pr4));

    And pr5 = FilterApi.and(pr1, pr2);
    assertFalse(pr1.equals(pr5));
  }
}
