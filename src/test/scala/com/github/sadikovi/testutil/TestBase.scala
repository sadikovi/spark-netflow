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

package com.github.sadikovi.testutil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.github.sadikovi.testutil.implicits._

trait TestBase {
  val RESOLVER = "path-resolver"

  var path: String = ""

  /** returns raw path of the folder where it finds resolver */
  private def getRawPath(): String = {
    if (path.isEmpty) {
      path = getClass().getResource("/" + RESOLVER).getPath()
    }
    path
  }

  /** base directory of the project */
  final protected def baseDirectory(): String = {
    val original = getRawPath().split("/")
    require(original.length > 4, s"Path length is too short (<= 4): ${original.length}")
    val base = original.dropRight(4)
    var dir = ""
    for (suffix <- base) {
      if (suffix.nonEmpty) {
        dir = dir / suffix
      }
    }
    dir
  }

  /** main directory of the project (./src/main) */
  final protected def mainDirectory(): String = {
    baseDirectory() / "src" / "main"
  }

  /** test directory of the project (./src/test) */
  final protected def testDirectory(): String = {
    baseDirectory() / "src" / "test"
  }

  /** target directory of the project (./target) */
  final protected def targetDirectory(): String = {
    baseDirectory() / "target"
  }

  final protected def mkdirs(path: String): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(new Configuration(false))
    fs.mkdirs(p)
  }

  /** delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: String, recursive: Boolean): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(new Configuration(false))
    fs.delete(p, recursive)
  }
}
