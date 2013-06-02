package com.mvdb.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.pig.piggybank.evaluation.math.ABS;

/**
 * 
 * 
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		System.out.println("Hello World!");
		logger.error("error");
		logger.warn("warning");
		logger.info("info");
	    // print internal state
	    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
	    StatusPrinter.print(lc);
	}
}
