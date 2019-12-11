package org.dice_research.anutch.adapter.system;

import static org.hobbit.core.Constants.CONTAINER_TYPE_SYSTEM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemAdapter extends AbstractSystemAdapter {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemAdapter.class);

	
	private final static String ANUTCH_IMAGE = "apache/nutch";
    protected boolean terminating = false;
    protected String[] NUTCH_ENV;

    
    protected String nutchInstance;

	
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
        
        
        	
        NUTCH_ENV = new String[]{ "b=10",
                "oe="+sparqlUrl,
                "o=tempFile",
                "user_sparql=" + sparqlUser,
                "passwd_sparql=" + sparqlPwd,
                "s="+String.join(",", seedURIs)};

        nutchInstance = createContainer(ANUTCH_IMAGE, CONTAINER_TYPE_SYSTEM, NUTCH_ENV);
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
        if ((nutchInstance != null) && (nutchInstance.equals(containerName)) && !terminating) {
            Exception e = null;
            if (exitCode != 0) {
                // The ldspider had an error. Its time to panic
                LOGGER.error("ldspider terminated with exit code {}.", exitCode);
                e = new IllegalStateException("ldspider terminated with exit code " + exitCode + ".");
            }
            nutchInstance = null;
            terminate(e);
        } 
		
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		super.close();
	}

}
