package ai.vital.vitalservice.impl;

import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionsExecutor;
import ai.vital.vitalsigns.model.VitalTransaction;

public class VitalServiceAdminTransactionsExecutor implements  TransactionsExecutor {

	AbstractVitalServiceAdminImplementation impl;

	public VitalServiceAdminTransactionsExecutor(
			AbstractVitalServiceAdminImplementation impl) {
		super();
		this.impl = impl;
	}

	@Override
	public void _supportCheck() throws VitalServiceUnimplementedException {
		this.impl._transactionsCheck();
	}
//
	@Override
	public void _createTransaction(VitalTransaction transaction)
			throws VitalServiceUnimplementedException, VitalServiceException {
		this.impl._createTransaction(transaction);
	}

	@Override
	public void _commitTransaction(TransactionWrapper transactionWrapper) throws VitalServiceException, VitalServiceUnimplementedException {
		this.impl._commitTransaction(transactionWrapper);
	}

	@Override
	public void _rollbackTransaction(TransactionWrapper transactionWrapper) throws VitalServiceException, VitalServiceUnimplementedException {
		this.impl._rollbackTransaction(transactionWrapper);
	}
	
}
