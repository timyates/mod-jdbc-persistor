/*
 * Copyright 2011-2012 the original author or authors.
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

package com.bloidonia.vertx.mods ;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;

import org.vertx.java.core.json.JsonArray ;

public class JsonUtils {
  public static List<Object> unwrapper( JsonArray array ) {
    List<Object> params = new ArrayList<Object>() ;
    Iterator<Object> iter = array.iterator() ;
    while( iter.hasNext() ) {
      params.add( iter.next() ) ;
    }
    return params ;
  }

  public static List<List<Object>> arrayNormaliser( JsonArray array ) {
    if( array == null ) {
      return null ;
    }
    Iterator<Object> iter = array.iterator() ;
    Object first = iter.next() ;
    List<List<Object>> ret = new ArrayList<List<Object>>() ;
    if( first instanceof JsonArray ) {
      ret.add( unwrapper( (JsonArray)first ) ) ;
      while( iter.hasNext() ) {
        ret.add( unwrapper( (JsonArray)iter.next() ) ) ;
      }
    }
    else {
      ret.add( unwrapper( array ) ) ;
    }
    return ret ;
  }
}