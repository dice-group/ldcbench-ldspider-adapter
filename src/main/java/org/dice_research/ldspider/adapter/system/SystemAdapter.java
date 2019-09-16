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

	
	private final static String LDSPIDER_IMAGE = "dicegroup/ldspider:latest";
    private long numberOfThreads = 2;
    protected boolean terminating = false;
    protected String[] LDSPIDER_ENV;
    public final static String NUMBER_THREADS_URI = "http://project-hobbit.eu/ldcbench-system/numberOfThreads";

    
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
        LOGGER.info("Seed URIs: {}.", Arrays.toString(seedURIs));
        
        Literal workerCountLiteral = RdfHelper.getLiteral(systemParamModel, null,
                systemParamModel.getProperty(NUMBER_THREADS_URI));
        if (workerCountLiteral == null) {
            throw new IllegalStateException(
                    "Couldn't find necessary parameter value for \"" + NUMBER_THREADS_URI + "\". Aborting.");
        }
        numberOfThreads = workerCountLiteral.getInt();
        	
        LDSPIDER_ENV = new String[]{ "b=10",
                "oe="+sparqlUrl,
                "user_sparql=" + sparqlUser,
                "passwd_sparql=" + sparqlPwd,
                "t="+numberOfThreads,
                "s="+String.join(",", seedURIs)};

        ldSpiderInstance = createContainer(LDSPIDER_IMAGE, CONTAINER_TYPE_SYSTEM, LDSPIDER_ENV);
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
		// TODO Auto-generated method stub
		super.close();
	}

}
