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

load('test_utils.js')
load('vertx.js')

var tu = new TestUtils();

var eb = vertx.eventBus;

function testInvalidAction() {
  eb.send( 'test.persistor', {
    action: 'blahblahblah'
  }, function( reply ) {
    tu.azzert( reply.status === 'error' ) ;
    tu.testComplete() ;
  } )
}

function testSimpleSelect() {
  eb.send( 'test.persistor', {
    action: 'select',
    stmt:   'SELECT * FROM INFORMATION_SCHEMA.SYSTEM_USERS'
  }, function( reply ) {
    console.log( reply ) ;
    tu.azzert( reply.status === 'ok' ) ;
    tu.azzert( reply.result != undefined ) ;
    tu.testComplete() ;
  } )
}

tu.registerTests(this);
var persistorConfig = { address: 'test.persistor' }
vertx.deployModule('vertx.jdbc-persistor-v' + java.lang.System.getProperty('vertx.version'), persistorConfig, 1, function() {
  tu.appReady();
});

function vertxStop() {
  tu.unregisterAll();
  tu.appStopped();
}