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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{Map => MutableMap}
import scala.language.existentials

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodeFormatter}
import org.codehaus.janino.ClassBodyEvaluator
import org.slf4j.LoggerFactory

import com.github.sadikovi.spark.util.Utils

/**
 * The content of the file is slightly modified version of `CodeGenerator` file from Apache Spark.
 * See: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/
 * sql/catalyst/expressions/codegen/CodeGenerator.scala
 * The removed parts relate to usage of `Expression`s and other Spark SQL classes. [[CodeGenerator]]
 * in this file is fairly simplified to provide just what needed for the package.
 */

/**
 * A wrapper for generated class with defined `generate` method so that potentially can be used to
 * pass some extra objects into generated class.
 */
abstract class GeneratedClass {
  def generate(): Any
}

/**
 * [[CodeGenerator]] is a simplified version of Spark `CodeGenerator` with replaced cache, since it
 * somehow requires CacheBuilder from guava library to be imported explicitly, though cache can be
 * easily replaced with `Map` in this case.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] {

  // CodeGenerator logger
  private val logger = LoggerFactory.getLogger(Utils.getLogName(getClass()))
  // A cache primitive for generated classes.
  private val cache = new ConcurrentHashMap[String, GeneratedClass]()

  protected def declareMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map { case (javaType, variableName, _) =>
      s"private $javaType $variableName;"
    }.mkString("\n")
  }

  protected def initMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map(_._3).mkString("\n")
  }

  // Map of added functions, each entry is "function name" -> "function code"
  private val addedFunctions: MutableMap[String, String] = MutableMap.empty[String, String]

  protected def addNewFunction(funcName: String, funcCode: String): Unit = {
    addedFunctions += ((funcName, funcCode))
  }

  protected def declareAddedFunctions(): String = {
    addedFunctions.map { case (funcName, funcCode) => funcCode }.mkString("\n")
  }

  /**
   * Generate a class for a given input expression. Input expression is already canonicalized at
   * this stage. Should call `compile()` to produce generated code.
   */
  protected def create(in: InType): OutType

  /**
   * Canonicalize an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  protected def canonicalize(in: InType): InType

  /** Compile the Java source code into a Java class with addition of cache */
  protected def compile(code: String): GeneratedClass = {
    if (cache.containsKey(code)) {
      logger.debug("Found key in cache")
      cache.get(code)
    } else {
      val startTime = System.nanoTime()
      val result = doCompile(code)
      val endTime = System.nanoTime()
      def timeMs: Double = (endTime - startTime).toDouble / 1000000
      logger.info(s"Code generated in $timeMs ms")
      cache.put(code, result)
      result
    }
  }

  /** Compile the Java source code into a Java class, using Janino. */
  private def doCompile(code: String): GeneratedClass = {
    val evaluator = new ClassBodyEvaluator()
    evaluator.setParentClassLoader(Utils.getContextClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("com.github.sadikovi.spark.netflow.codegen.expressions.GeneratedClass")
    evaluator.setDefaultImports(Array.empty)
    evaluator.setExtendedClass(classOf[GeneratedClass])

    def formatted = CodeFormatter.format(code)

    try {
      evaluator.cook("generated.java", code)
      logger.info(s"Formatted code: \n$formatted")
    } catch {
      case e: Exception =>
        val msg = s"Failed to compile: $e\n$formatted"
        throw new Exception(msg, e)
    }

    evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass]
  }

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expression: InType): OutType = create(canonicalize(expression))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext
  }

  /** Remove key from cache */
  final def evict(key: String): Unit = cache.remove(key)

  /** Clear cache */
  final def clearCache(): Unit = cache.clear()
}
