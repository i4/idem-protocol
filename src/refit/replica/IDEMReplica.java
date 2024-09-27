package refit.replica;

import refit.agreement.idem.IDEMMessage.IDEMCheckpointMessage;
import refit.agreement.idem.IDEMMessage.IDEMCheckpointRequest;
import refit.agreement.idem.IDEMMessage.IDEMCommit;
import refit.agreement.idem.IDEMMessage.IDEMForwardRequest;
import refit.agreement.idem.IDEMMessage.IDEMPropose;
import refit.agreement.idem.IDEMMessage.IDEMRequire;
import refit.agreement.idem.IDEMMessage.IDEMViewChangeMessage;
import refit.agreement.idem.execution.IDEMExecutionStage;
import refit.agreement.idem.order.IDEMOrderStage;
import refit.application.REFITApplicationServer;
import refit.message.REFITRequest;
import refit.scheduler.REFITScheduler;
import refit.scheduler.REFITSchedulerTaskType;

public class IDEMReplica extends REFITBaseReplica {

	private final IDEMExecutionStage executionStage;
	private final IDEMOrderStage orderStage;

	public IDEMReplica(short id, boolean isSingleThreaded) {
		super(id, isSingleThreaded, true);

		// Create and initialize application
		final REFITApplicationServer application = createApplication();
		application.init();

		// Setup stages
		this.executionStage = new IDEMExecutionStage(context, application);
		this.orderStage = new IDEMOrderStage(context, executionStage.checkpoint);

		// Configure connections and senders/listeners
		addToClientInbound(executionStage);
		addClientInbound(REFITRequest.class);

		addToInbound(IDEMForwardRequest.class, executionStage);
		addToInbound(IDEMCommit.class, executionStage);
		addToInbound(IDEMCheckpointRequest.class, executionStage);
		addToInbound(IDEMCheckpointMessage.class, executionStage);

		addToInbound(IDEMRequire.class, orderStage);
		addToInbound(IDEMViewChangeMessage.class, orderStage);
		addToInbound(IDEMPropose.class, orderStage);

		this.executionStage.checkpointSender.addListener(orderStage);
		this.executionStage.viewSender.addListener(orderStage);
		this.executionStage.requireSender.addListener(orderStage);

		this.orderStage.viewSender.addListener(executionStage);
		this.orderStage.commitSender.addListener(executionStage);
	}

	@Override
	protected void assignTasks(REFITScheduler[] schedulers) {
		schedulers[0 % schedulers.length].assignTasks(group, REFITSchedulerTaskType.NETWORK_ENDPOINT);
		schedulers[0 % schedulers.length].assignTasks(group, REFITSchedulerTaskType.CLIENT_NETWORK_ENDPOINT_WORKER);
		schedulers[1 % schedulers.length].assignTasks(group, REFITSchedulerTaskType.REPLICA_NETWORK_ENDPOINT_WORKER);
		schedulers[1 % schedulers.length].assignTasks(group, REFITSchedulerTaskType.ORDER_STAGE);
		schedulers[1 % schedulers.length].assignTasks(group, REFITSchedulerTaskType.EXECUTION_STAGE);
	}

}
