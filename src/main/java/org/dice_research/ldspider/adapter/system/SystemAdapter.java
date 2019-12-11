package org.dice_research.ldspider.adapter.system;

import static org.hobbit.core.Constants.CONTAINER_TYPE_SYSTEM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.jena.rdf.model.Literal;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.utils.rdf.RdfHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemAdapter extends AbstractSystemAdapter {

	private static final Logger LOGGER = LoggerFactory.getLogger(SystemAdapter.class);

	private final static String LDSPIDER_IMAGE = "dicegroup/ldspider-filesink:latest";
	private long numberOfThreads = 2;
	private String strategy = "";
	protected boolean terminating = false;
	protected String[] LDSPIDER_ENV;
	public final static String NUMBER_THREADS_URI = "http://project-hobbit.eu/ldcbench-system/numberOfThreads";
	public final static String STRATEGY_URI = "http://project-hobbit.eu/ldcbench-system/strategy";

	protected String ldSpiderInstance;

	@Override
	public void init() throws Exception {
		super.init();

	}

	@Override
	public void receiveGeneratedData(byte[] data) {
		// handle the incoming data as described in the benchmark description
		ByteBuffer buffer = ByteBuffer.wrap(data);
		String sparqlUrl = RabbitMQUtils.readString(buffer);
		String sparqlUser = RabbitMQUtils.readString(buffer);
		String sparqlPwd = RabbitMQUtils.readString(buffer);
		String[] seedURIs = RabbitMQUtils.readString(buffer).split("\n");

		LOGGER.info("Sparql Endpoint: " + sparqlUrl);
		LOGGER.info("Sparql User: " + sparqlUser);
		LOGGER.info("Sparql Passwd: " + sparqlPwd);

		
		LOGGER.info("Seed URIs: {}.", Arrays.toString(seedURIs));

		Literal workerCountLiteral = RdfHelper.getLiteral(systemParamModel, null,
				systemParamModel.getProperty(NUMBER_THREADS_URI));
		
		Literal strategyLiteral = RdfHelper.getLiteral(systemParamModel, null,
				systemParamModel.getProperty(STRATEGY_URI));
		
		if (workerCountLiteral == null) {
			throw new IllegalStateException(
					"Couldn't find necessary parameter value for \"" + NUMBER_THREADS_URI + "\". Aborting.");
		}
		numberOfThreads = workerCountLiteral.getInt();
		strategy = strategyLiteral.getString();

		if(strategy != null && strategy.equals("b")) {
			LOGGER.info("Using breadth-first Strategy");
			LDSPIDER_ENV = new String[]{ "b=-1",
	                "oe="+sparqlUrl,
	                "o=tempFile",
	                "user_sparql=" + sparqlUser,
	                "passwd_sparql=" + sparqlPwd,
	                "t="+numberOfThreads,
	                "s="+String.join(",", seedURIs)};
		}else if(strategy != null && strategy.equals("c")) {
			LOGGER.info("Using load balanced Strategy");
			LDSPIDER_ENV = new String[]{ "c=1000000000",
	                "oe="+sparqlUrl,
	                "o=tempFile",
	                "user_sparql=" + sparqlUser,
	                "passwd_sparql=" + sparqlPwd,
	                "t="+numberOfThreads,
	                "s="+String.join(",", seedURIs)};
		}
		

		LOGGER.info("Starting LDSpider - FileSink");
		ldSpiderInstance = createContainer(LDSPIDER_IMAGE, CONTAINER_TYPE_SYSTEM, LDSPIDER_ENV);
		LOGGER.info("Image Started");

	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		throw new IllegalStateException("Should not receive any tasks.");
	}

	@Override
	protected synchronized void terminate(Exception cause) {
		LOGGER.debug("Terminating");
		terminating = true;
		super.terminate(cause);
	}

	@Override
	public void receiveCommand(byte command, byte[] data) {
		if (command == Commands.DOCKER_CONTAINER_TERMINATED) {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			String containerName = RabbitMQUtils.readString(buffer);
			int exitCode = buffer.get();
			containerStopped(containerName, exitCode);
		}
		super.receiveCommand(command, data);
	}

	public void containerStopped(String containerName, int exitCode) {
		// Check whether it is one of your containers and react accordingly
		if ((ldSpiderInstance != null) && (ldSpiderInstance.equals(containerName)) && !terminating) {
			Exception e = null;
			if (exitCode != 0) {
				// The ldspider had an error. Its time to panic
				LOGGER.error("ldspider terminated with exit code {}.", exitCode);
				e = new IllegalStateException("ldspider terminated with exit code " + exitCode + ".");
			}
			ldSpiderInstance = null;
			terminate(e);
		}

	}

	@Override
	public void close() throws IOException {
		LOGGER.info("Finishing LDSpider File Sink adapter");
		super.close();
	}

}
