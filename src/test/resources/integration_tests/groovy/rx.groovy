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
package integration_tests.groovy

import io.vertx.rxcore.groovy.eventbus.RxEventBus

import org.vertx.groovy.testframework.TestUtils
import org.vertx.groovy.testtools.VertxTests

import static org.vertx.testtools.VertxAssert.*

def config = [ address: 'test.persistor',
               url:     'jdbc:hsqldb:mem:testdb?shutdown=true' ]

VertxTests.initialize( this )

rxEventBus = new RxEventBus( vertx.eventBus )

container.deployModule( System.getProperty("vertx.modulename"), config, 1 ) { asyncResult ->
  System.out.println( asyncResult )
  VertxTests.startTests( this )
}

def testRxMapMany() {
  testComplete()
/*
   Waiting for RxEventBus support for Groovy to mature

  rxEventBus.send( "test.persistor", [ 
               action:  'execute',
               stmt:    '''CREATE TABLE IF NOT EXISTS test ( 
                          |  id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
                          |  name VARCHAR(80),
                          |  age INTEGER,
                          |  CONSTRAINT testid PRIMARY KEY ( id ) )'''.stripMargin() ] )
            .mapMany { message ->
              assertEquals( message.body().getString( "status" ), "ok" )
              rxEventBus.send( "test.persistor", [
                action:  'insert',
                stmt:    'INSERT INTO test ( name, age ) VALUES ( ?, ? )',
                values:  [ [ 'tim', 65 ], [ 'dave', 29 ], [ 'mike', 42 ] ] ] )
            }
            .mapMany { message ->
              assertEquals( message.body().getString( "status" ), "ok" )
              assertEquals( message.body().getArray( "result" )*.ID, [ 1, 2, 3 ] )
              rxEventBus.send( "test.persistor", [
                action:  'select',
                stmt:    'SELECT * FROM test ORDER BY age ASC' ] )
            }
            .mapMany { message ->
              assertEquals( message.body().getString( "status" ), "ok" )
              assertEquals( message.body().getArray( "result" )*.AGE, [ 29, 42, 65 ] )
              rxEventBus.send( "test.persistor", [
                action:  'execute',
                stmt:    'DROP TABLE test' ] )
            }
            .subscribe { message ->
              assertEquals( message.body().getString( "status" ), "ok" )
              System.out.println "ALL OK!! :-)"
              testComplete()
            }
*/
}


