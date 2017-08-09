package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.rules.TestRule;

/**
 * Provides an Accumulo instance to a test method.
 * Handles setup and teardown.
 */
public interface IAccumuloTester extends TestRule {


    Connector getConnector();
    String getUsername();
    PasswordToken getPassword();


}
