package ai.vital.vitalservice.superadmin.impl;

import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionWrapper;
import ai.vital.vitalservice.impl.TransactionsImplementation.TransactionsExecutor;
import ai.vital.vitalservice.superadmin.VitalServiceSuperAdmin;
import ai.vital.vitalsigns.model.VitalTransaction;

public class VitalServiceSuperAdminTransactionsExecutor implements
		TransactionsExecutor {

	private AbstractVitalServiceSuperAdminImplementation vitaserviceSuperAdmin;
	
	public VitalServiceSuperAdminTransactionsExecutor(
			AbstractVitalServiceSuperAdminImplementation vitaserviceSuperAdmin) {
		super();
		this.vitaserviceSuperAdmin = vitaserviceSuperAdmin;
	}

	@Override
	public void _supportCheck() throws VitalServiceUnimplementedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void _createTransaction(VitalTransaction transaction)
			throws VitalServiceUnimplementedException, VitalServiceException {
		// TODO Auto-generated method stub

	}

	@Override
	public void _commitTransaction(TransactionWrapper transactionWrapper)
			throws VitalServiceException, VitalServiceUnimplementedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void _rollbackTransaction(TransactionWrapper transactionWrapper)
			throws VitalServiceException, VitalServiceUnimplementedException {
		// TODO Auto-generated method stub

	}

}
