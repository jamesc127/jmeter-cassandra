package org.apache.cassandra.jmeter.config;

/*
 * Copyright 2014 Steven Lowenthal
 * Updates Copyright 2019 James Colvin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.datastax.dse.driver.api.core.DseSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class CassandraSessionFactory {

  // This class supports both multiple cluster objects for different clusters, as well as
  // multiple sessions to the same cluster.

  // This does not support multiple cluster objects to the same host that differ in
  // parameters

  // TODO - When do we shut down a session or cluster??

  static CassandraSessionFactory instance;
  final Map<String, DseSession> sessions = new HashMap<String, DseSession>();

  private void CassandraSessionFactory() {

  }

  public static synchronized CassandraSessionFactory getInstance() {
    if(instance == null) {
      instance = new CassandraSessionFactory();
    }
    return instance;
  }

  public static synchronized DseSession createSession(String sessionKey, Set<InetAddress> host,
                                                      String keyspace, String username, String password/*, LoadBalancingPolicy loadBalancingPolicy*/) {
    instance = getInstance();
    DseSession session = instance.sessions.get(sessionKey);
    Iterator<InetAddress> iHost = host.iterator();
    Collection<InetSocketAddress> contactPoints = null;
    while (iHost.hasNext()){
        contactPoints.add(new InetSocketAddress(iHost.next(),9042));
    }
    if (session == null) {
        if (keyspace == null && username == null && password == null)
            session = DseSession.builder().addContactPoints(contactPoints).build();
        else if (keyspace == null && username != null && password != null)
            session = DseSession.builder().addContactPoints(contactPoints).withAuthCredentials(username,password).build();
        else if (keyspace != null && username == null && password == null)
            session = DseSession.builder().addContactPoints(contactPoints).withKeyspace(keyspace).build();
        else session = DseSession.builder().addContactPoints(contactPoints).build();
        instance.sessions.put(sessionKey, session);
    }
    return session;
  }

  public static synchronized void destroyClusters() {
      for (DseSession session : instance.sessions.values()) {
          session.close();
      }
      instance.sessions.clear();
  }

  public static synchronized void closeSession(DseSession session) {

      // Find the session
      for (Map.Entry<String, DseSession> entry : instance.sessions.entrySet()) {
           if (entry.getValue() == session) {
               session.close();
               instance.sessions.remove(entry.getKey());
               return;
           }
      }

      assert false: "Closing session that is not found";
  }

}
