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

package com.github.sadikovi.netflowlib.codegen;

/**
 * Node for code generation, essentially maps to a single variable. Needs to provide name of the
 * variable and type, so we can reconstruct assignment.
 */
public interface CodeGenNode {
  /** Get name of the node */
  public String nodeName(CodeGenContext ctx);

  /** Get Java type of the node */
  public String nodeType(CodeGenContext ctx);
}
