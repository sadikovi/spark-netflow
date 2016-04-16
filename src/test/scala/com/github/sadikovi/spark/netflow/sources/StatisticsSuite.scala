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

package com.github.sadikovi.spark.netflow.sources

import com.github.sadikovi.testutil.UnitTestSpec

class StatisticsSuite extends UnitTestSpec {
  test("resolve statistics path without root") {
    val resolver = StatisticsPathResolver(None)
    val path = resolver.getStatisticsPath("file:/x/y/z/file")
    path should be ("file:/x/y/z/.statistics-file")
  }

  test("resolve statistics path with root") {
    val resolver = StatisticsPathResolver(Some("file:/a/b/c"))
    val path = resolver.getStatisticsPath("file:/x/y/z/file")
    path should be ("file:/a/b/c/x/y/z/.statistics-file")
  }

  test("fail if root path is null") {
    intercept[IllegalArgumentException] {
      StatisticsPathResolver(Some(null))
    }
  }

  test("fail if root path is empty") {
    intercept[IllegalArgumentException] {
      StatisticsPathResolver(Some(""))
    }
  }

  test("fail if file path is null") {
    val resolver = StatisticsPathResolver(None)
    intercept[IllegalArgumentException] {
      resolver.getStatisticsPath(null)
    }
  }

  test("fail if file path is empty") {
    val resolver = StatisticsPathResolver(None)
    intercept[IllegalArgumentException] {
      resolver.getStatisticsPath("")
    }
  }
}
