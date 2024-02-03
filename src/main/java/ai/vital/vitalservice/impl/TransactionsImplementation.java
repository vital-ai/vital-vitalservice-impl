package ai.vital.vitalservice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.vitalservice.TransactionOperation;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.exception.VitalServiceUnimplementedException;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionState;

//common transactions implementation for all endpoint types


public class TransactionsImplementation {

	private final static Logger log = LoggerFactory.getLogger(TransactionsImplementation.class);
	
	private TransactionsExecutor executor;
	
	public TransactionsImplementation(TransactionsExecutor executor) {
		super();
		this.executor = executor;
	}

	private Map<String, TransactionWrapper> transactionsMap = Collections.synchronizedMap( new  HashMap<String, TransactionWrapper>() );
	
	public static class TransactionWrapper {
		
		public VitalTransaction transaction;
		
		public List<TransactionOperation> operations = new ArrayList<TransactionOperation>();

		public String lastError;

		public TransactionWrapper(VitalTransaction transaction) {
			super();
			this.transaction = transaction;
		}
		
		public String getID() {
			return (String) transaction.getRaw(Property_hasTransactionID.class);
		}
		
		public String getState() {
			return (String) transaction.getRaw(Property_hasTransactionState.class);
		}
	}
	
	
	public static interface TransactionsExecutor {
		
		/**
		 * All endpoints that don't support transactions should throw VitalServiceUnimplementedException 
		 * @throws VitalServiceUnimplementedException
		 */
		public void _supportCheck() throws VitalServiceUnimplementedException;
		
		/**
		 * The executor should create transaction resource, generate the identifier and set it in transaction object
		 * @param transaction
		 */
		public void _createTransaction(VitalTransaction transaction) throws VitalServiceUnimplementedException, VitalServiceException;

		public void _commitTransaction(TransactionWrapper transactionWrapper) throws VitalServiceException, VitalServiceUnimplementedException;

		public void _rollbackTransaction(TransactionWrapper transactionWrapper) throws VitalServiceException, VitalServiceUnimplementedException;
		
	}
	
	public void rollbackAllTransactions() {

		List<TransactionWrapper> r = new ArrayList<TransactionWrapper>();
		
		synchronized(transactionsMap) {
			
			for(TransactionWrapper tw : transactionsMap.values()) {
				
				String state = tw.getState();
				if("OPEN".equals(state)) {
					r.add(tw);
				}
				
			}
			
		}
		
		for(TransactionWrapper w : r) {
			try {
				executor._rollbackTransaction(w);
			} catch(Exception e) {
				
			}
		}
		
	}

	
	public void blockOnTransactionID(String txID) throws VitalServiceException {

		log.info("blocking on transaction with ID: {}", txID);
		
		TransactionWrapper tx = null;
		
		synchronized (transactionsMap) {
		
			tx = transactionsMap.get(txID);
			
			if(tx == null) throw new VitalServiceException("No active transaction with ID: " + txID + " found");
			
			String state = tx.getState();
			if(!state.equals("OPEN")) throw new VitalServiceException("Invalid transaction state: " + state + ", expected: OPEN");
			
		}

		log.info("Tx found: {} - blocking on it ...", txID);
		
		synchronized (tx) {
			
			synchronized (transactionsMap) {
				if(!transactionsMap.containsKey(txID)) {
					log.info("Tx has just completed, thread won't sleep, {}", txID);
					return;
				}
			}
				
			
			try {
				tx.wait();
			} catch (InterruptedException e) {
				throw new VitalServiceException(e.getLocalizedMessage());
			}
			
		}
		
		log.info("Transaction finished {}, awakening thread", txID);
		
		
	}
	
	
	public VitalTransaction createTransaction() throws VitalServiceUnimplementedException, VitalServiceException {
		
		executor._supportCheck();
		
		VitalTransaction transaction = new VitalTransaction();
		transaction.generateURI((VitalApp) null);
		transaction.set(Property_hasTransactionState.class, "OPEN");

		try {
			executor._createTransaction(transaction);
		} catch (Exception e) {
			throw new VitalServiceException(e);
		}

		String transactionID = (String) transaction.getRaw(Property_hasTransactionID.class);
		
		if(transactionID == null || transactionID.isEmpty()) throw new VitalServiceException("Implementation didn't set the transaction ID");
		
		try {
			
			synchronized (transactionsMap) {
				
				if(transactionsMap.containsKey(transactionID)) throw new VitalServiceException("TransactionID already exists: " + transactionID);
				
				transactionsMap.put(transactionID, new TransactionWrapper(transaction));
				
			}
			
		} catch(Exception e) {
			//TODO rollback transaction
//			executor._rollb
			throw e;
		}
		
		return transaction;
	}

	public VitalStatus commitTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException{

		executor._supportCheck();
		
		String tID = (String) transaction.getRaw(Property_hasTransactionID.class);
		if(tID == null || tID.isEmpty()) throw new NullPointerException("Transaction has no transactionID property");
		
		TransactionWrapper transactionWrapper = transactionsMap.get(tID);
		if(transactionWrapper == null) {
			throw new VitalServiceException("Transaction not found, ID: " + tID);
		}
		
		String state = transactionWrapper.getState();
		if(!state.equals("OPEN")) throw new VitalServiceException("Invalid transaction state: " + state + ", expected: OPEN");
		
//		try {
			
			executor._commitTransaction(transactionWrapper);
			
//		} catch(Exception e) {
//			
//			transactionWrapper.lastError = e.getLocalizedMessage();
//			transactionWrapper.transaction.set(Property_hasTransactionState.class, "FAILED");
//			throw e;
//			
//		}

		transactionWrapper.transaction.set(Property_hasTransactionState.class, "COMMITTED");
		
		TransactionWrapper tx = transactionsMap.remove(transactionWrapper.getID());
		
		if(tx != null) {
			
			synchronized (tx) {
				tx.notifyAll();
			}
			
		}
		
//		transaction.setState(TransactionState.COMMITTED);
//		currentTransaction = null;
		return VitalStatus.withOKMessage("Transaction committed");
		
		//ok
	}

	public VitalStatus rollbackTransaction(VitalTransaction transaction) throws VitalServiceException, VitalServiceUnimplementedException {
		
		executor._supportCheck();
		
		String tID = (String) transaction.getRaw(Property_hasTransactionID.class);
		if(tID == null || tID.isEmpty()) throw new NullPointerException("Transaction has no transactionID property");
		
		TransactionWrapper transactionWrapper = transactionsMap.get(tID);
		if(transactionWrapper == null) {
			throw new VitalServiceException("Transaction not found, ID: " + tID);
		}
		
		String state = transactionWrapper.getState();
		if(!state.equals("OPEN")) throw new VitalServiceException("Invalid transaction state: " + state + ", expected: OPEN");
		
		executor._rollbackTransaction(transactionWrapper);
		transactionWrapper.transaction.set(Property_hasTransactionState.class, "ROLLED_BACK");
		
		TransactionWrapper tx = transactionsMap.remove(transactionWrapper.getID());
		
		if(tx != null) {
			
			synchronized (tx) {
				tx.notifyAll();
			}
			
		}
		
		return VitalStatus.withOKMessage("Transaction rolled back");
		
//		} catch(Exception e) {
//			return VitalStatus.withError(e.getLocalizedMessage());
//		}
		
//		if(currentTransaction == null) throw new RuntimeException("No active transaction");
//
//		if(!currentTransaction.getID().equals(transaction.getID())) 
//			throw new RuntimeException("Current transaction is different than provided: " + currentTransaction.getID() + " vs " + transaction.getID());
//
//		try {
//			_rollbackTransaction(transaction);
//			transaction.setState(TransactionState.ROLLED_BACK);
//			currentTransaction = null;
//			return VitalStatus.withOKMessage("Transaction rolled back");
//		} catch(Exception e) {
//			return VitalStatus.withError(e.getLocalizedMessage());
//		}
	}

	public List<VitalTransaction> listTransactions() {

		List<VitalTransaction> r = new ArrayList<VitalTransaction>();
		
		synchronized (transactionsMap) {
			
			for(TransactionWrapper w : transactionsMap.values()) {
				r.add(w.transaction);
			}
		}
		
		return r;
		
	}
	
	public TransactionWrapper getWrapped(String transactionID) {
		return transactionsMap.get(transactionID);
	}

	public void addTransactionOperation(VitalTransaction transaction,
			TransactionOperation dop) throws VitalServiceException, VitalServiceUnimplementedException {
		// TODO Auto-generated method stub
		
		executor._supportCheck();
		
		String tID = (String) transaction.getRaw(Property_hasTransactionID.class);
		if(tID == null || tID.isEmpty()) throw new NullPointerException("Transaction has no transactionID property");
		
		TransactionWrapper transactionWrapper = transactionsMap.get(tID);
		if(transactionWrapper == null) {
			throw new VitalServiceException("Transaction not found, ID: " + tID);
		}
		
		transactionWrapper.operations.add(dop);
		
	}

	
	
}
