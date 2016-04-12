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

package com.github.sadikovi.spark.netflow.codegen

import java.util.{Iterator => JIterator}

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext

import com.github.sadikovi.spark.netflow.sources.ConvertFunction

abstract class AbstractCodeGenIterator {
  def eval(r: JIterator[Array[Object]]): JIterator[Array[Object]]
}

/**
 * [[GenerateIterator]] converts array of String functions with indices into generated iterator
 * function to apply "stringify" on each listed (as index) field. Functions added to generated code
 * are distinct, if 10 fields are fetched when the same function is applied, only one function is
 * added.
 */
object GenerateIterator extends CodeGenerator[Array[(ConvertFunction, Array[Int])],
    (JIterator[Array[Object]]) => JIterator[Array[Object]]] {
  // Internal input type
  type InType = Array[(ConvertFunction, Array[Int])]
  type OutType = (JIterator[Array[Object]]) => JIterator[Array[Object]]

  // All conversion functions are supposed to return {Any => String} direct function, which
  // translates into {Object => String} for codegen.
  protected def create(arr: InType): ((JIterator[Array[Object]]) => JIterator[Array[Object]]) = {
    val ctx = newCodeGenContext()
    // Unique index is kept to generate unique function name
    var funcIndex: Int = 0
    // Statements for conversion functions
    val statements = new StringBuilder()

    for (entry <- arr) {
      val func = entry._1
      val funcName = s"func$funcIndex"
      val code = s"""
        public java.lang.String $funcName(java.lang.Object r) {
          ${func.gen(ctx)}
        }
      """
      addNewFunction(funcName, code)
      // create mapping for array elements
      val seq = entry._2
      for (elem <- seq) {
        statements.append(s"arr[$elem] = $funcName(arr[$elem]);")
        statements.append("\n")
      }
      funcIndex = funcIndex + 1
    }

    val code = s"""
      public SpecialMapPartitions generate() {
        return new SpecialMapPartitions();
      }

      class SpecialMapPartitions extends ${classOf[AbstractCodeGenIterator].getName} {
        ${declareMutableStates(ctx)}
        private java.util.Iterator<java.lang.Object[]> iter;

        public SpecialMapPartitions() {
          ${initMutableStates(ctx)}
          iter = null;
        }

        ${declareAddedFunctions()}

        public java.util.Iterator<java.lang.Object[]> iterator() {
          return new java.util.Iterator<java.lang.Object[]>() {
            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public java.lang.Object[] next() {
              java.lang.Object[] arr = (java.lang.Object[]) iter.next();
              ${statements.toString()}

              return arr;
            }

            @Override
            public void remove() {
              throw new java.lang.UnsupportedOperationException(
                "Remove operation is not supported");
            }
          };
        }

        public java.util.Iterator<java.lang.Object[]> eval(
            java.util.Iterator<java.lang.Object[]> iter) {
          this.iter = iter;
          return iterator();
        }
      }"""
    val p = compile(code).generate().asInstanceOf[AbstractCodeGenIterator]
    (r: JIterator[Array[Object]]) => p.eval(r)
  }

  protected def canonicalize(in: InType): InType = in
}
