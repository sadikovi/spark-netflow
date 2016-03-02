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

package com.github.sadikovi.netflowlib.util;

import java.net.URL;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal logging interface */
public abstract class Logging {
  protected Logger log = LoggerFactory.getLogger(getClass().getCanonicalName());

  protected Logging() {
    initLogging();
  }

  private void initLogging() {
    URL url = getClass().getResource("/com/github/sadikovi/netflowlib/log4j-defaults.properties");
    if (url == null) {
      // style: off
      System.out.println("Could not load configuration file, using basic configuration");
      // style: on
      BasicConfigurator.configure();
    } else {
      PropertyConfigurator.configure(url);
    }
  }
}
