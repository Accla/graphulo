package edu.mit.ll.sql.connection;

import java.net.URL;
import java.util.Properties;

public class SQLProperties {

	private static Properties props;

	public static void init(String filename) {
		props = new Properties();

		// load data from the properties file
		try {
			URL url = ClassLoader.getSystemResource(filename);
			
			if(url == null) 
				url = Thread.currentThread().getContextClassLoader().getResource(filename);				

			if(url == null)
				url = SQLProperties.class.getClassLoader().getResource(filename);
						
			System.out.println("trying to load: " + url);
			props.load(url.openStream());
			System.out.println("just loaded: " + url);
		}
		catch (Exception e) {
			e.printStackTrace();
			
		}
	}

	public static Object get(Object key) {
		// load the properties file if not already loaded
		if (props == null) {
			init("sql.properties");
		}

		// return the value asked for
		return props.get(key);
	}
}

/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
