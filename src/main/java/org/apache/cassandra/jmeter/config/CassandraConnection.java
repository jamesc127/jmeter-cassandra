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
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

public class CassandraConnection extends AbstractTestElement
    implements ConfigElement, TestStateListener, TestBean
    {

    // Load Balancer constants
    public static final String ROUND_ROBIN = "RoundRobin";
    public static final String DC_AWARE_ROUND_ROBIN = "DCAwareRoundRobin";
    public static final String DC_TOKEN_AWARE = "TokenAware(DCAwareRoundRobin)";
    public static final String WHITELIST = "WhiteList";
    public static final String DEFAULTLOADBALANCER = "Default";

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final long serialVersionUID = 233L;

    private transient String contactPoints, keyspace, username, password, sessionName, loadBalancer, localDataCenter;

    private final transient Set<InetAddress> contactPointsI = new HashSet<InetAddress>();
    private final transient Set<InetSocketAddress> contactPointsIS = new HashSet<InetSocketAddress>();

    // TODO - Add Port Number

    /*
     *  The datasource is set up by testStarted and cleared by testEnded.
     *  These are called from different threads, so access must be synchronized.
     *  The same instance is called in each case.
    */

    // Keep a record of the pre-thread pools so that they can be disposed of at the end of a test

    public CassandraConnection() {
    }

    public void testEnded() {
          CassandraSessionFactory.destroyClusters();
    }

    public void testEnded(String host) {
        testEnded();
    }

    public void testStarted() {
        this.setRunningVersion(true);
        TestBeanHelper.prepare(this);
        JMeterVariables variables = getThreadContext().getVariables();

        DseSession session = DseSession.builder().addContactPoints(contactPointsIS).withKeyspace(keyspace).withAuthCredentials(username, password).build();

        variables.putObject(sessionName, session);
    }

    public void testStarted(String host) {
        testStarted();
    }

    @Override
    public Object clone() {
        return (CassandraConnection) super.clone();
    }

    /*
     * Utility routine to get the connection from the pool.
     * Purpose:
     * - allows CassandraSampler to be entirely independent of the pooling classes
     * - allows the pool storage mechanism to be changed if necessary
     */
    public static DseSession getSession(String sessionName) {
                return (DseSession) JMeterContextService.getContext().getVariables().getObject(sessionName);
     }

    // used to hold per-thread singleton connection pools
    private static final ThreadLocal<Map<String, DseSession>> perThreadPoolMap =
        new ThreadLocal<Map<String, DseSession>>(){
        @Override
        protected Map<String, DseSession> initialValue() {
            return new HashMap<String, DseSession>();
        }
    };



    public void addConfigElement(ConfigElement config) {
    }

    public boolean expectsModification() {
        return false;
    }

    /**
     * @return Returns the poolname.
     */
    public String getContactPoints() {
        return contactPoints;
    }

    /**
     * @param contactPoints
     *            The poolname to set.
     */
    public void setContactPoints(String contactPoints) throws UnknownHostException {
        this.contactPoints = contactPoints;
        for (String contactPt : contactPoints.split(",")) {
            this.contactPointsI.add(InetAddress.getByName(contactPt));
            // TODO - 9160 should not really be hard coded.
            this.contactPointsIS.add(new InetSocketAddress(contactPt, 9042));
        }
    }

    /**
     * @return Returns the keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * @param keyspace
     *            The keyspace to set.
     */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password
     *            The password to set.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username
     *            The username to set.
     */
    public void setUsername(String username) {
        this.username = username;
    }


   public String getSessionName() {
       return sessionName;
   }

   public void setSessionName(String sessionName) {
       this.sessionName = sessionName;
   }

   public String getLoadBalancer() {
       return loadBalancer;
   }

   public void setLoadBalancer(String loadBalancer) {
       this.loadBalancer = loadBalancer;
   }

    public String getLocalDataCenter() {
        return localDataCenter;
    }

    public void setLocalDataCenter(String localDataCenter) {
        this.localDataCenter = localDataCenter;
    }
 }
