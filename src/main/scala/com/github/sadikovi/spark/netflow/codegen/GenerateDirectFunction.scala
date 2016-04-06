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

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext

import com.github.sadikovi.spark.netflow.sources.ConvertFunction

/**
 * Abstract [[DirectFunction]] class for code generated function. Essentially is just a mirror of
 * [[ConvertFunction]] `direct()` function that converts primitive into String, which is expressed
 * through `eval()`.
 */
abstract class DirectFunction {
  def eval(r: Any): String
}

/**
 * [[GenerateDirectFunction]] interface is an implementation of simple [[CodeGenerator]]. Allows
 * to convert create generated version of [[ConvertFunction]] `direct()` function.
 */
object GenerateDirectFunction extends CodeGenerator[ConvertFunction, (Any) => String] {
  protected def create(func: ConvertFunction): ((Any) => String) = {
    val ctx = newCodeGenContext()
    val funcBlock = func.gen(ctx)
    val code = s"""
      public SpecificFunc generate() {
        return new SpecificFunc();
      }

      class SpecificFunc extends ${classOf[DirectFunction].getName} {
        ${declareMutableStates(ctx)}
        public SpecificFunc() {
          ${initMutableStates(ctx)}
        }
        ${funcBlock}
      }"""
    val p = compile(code).generate().asInstanceOf[DirectFunction]
    (r: Any) => p.eval(r)
  }

  protected def canonicalize(in: ConvertFunction): ConvertFunction = in
}
