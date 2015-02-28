package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.rules.TestRule;

import java.util.Collection;

/**
 * Provides an Accumulo instance to a test method.
 * Handles setup and teardown.
 */
public interface IAccumuloTester extends TestRule {


    public Connector getConnector();
    public String getUsername();
    public PasswordToken getPassword();


}
