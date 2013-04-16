/*
 * Copyright 2012-2013 the original author or authors.
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

package com.bloidonia.vertx.mods.integration.javascript ;

import org.vertx.testtools.ScriptClassRunner;
import org.vertx.testtools.TestVerticleInfo;
import org.vertx.testtools.VertxAssert;
import org.junit.Test;
import org.junit.runner.RunWith;

@TestVerticleInfo(filenameFilter=".+\\.js", funcRegex="function[\\s]+(test[^\\s(]+)")
@RunWith(ScriptClassRunner.class)
public class JavaScriptIntegrationTests {

  public static int sleep( int seconds, int id ) {
    try {
      Thread.sleep( seconds * 1000 ) ;
      return id ;
    }
    catch( Exception e ) {
      return -id ;
    }
  }
}