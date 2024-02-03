package ai.vital.prime.service.model

/**
 * Default settings set to 1
 */
class SegmentProvisioning implements Serializable {

	static final long serialVersionUID = 1L;
	
	
	Long Properties_read = 1;
	Long Properties_write = 1;

	Long Properties_string_index_read = 1;
	Long Properties_string_index_write = 1;

	Long Properties_number_index_read = 1;
	Long Properties_number_index_write = 1;
	
	
	Boolean Node_stored             = true
	Boolean Node_indexed            = false
	
	Long Node_read = 1
	Long Node_write = 1
	
	
	
	
	Boolean Edge_stored       = true
	Boolean Edge_indexed      = false
	
	Long Edge_read = 1
	Long Edge_write = 1

		

	
	Boolean HyperNode_stored  = true
	Boolean HyperNode_indexed = false
	
	Long HyperNode_read = 1
	Long HyperNode_write = 1

	
	
	
	Boolean HyperEdge_stored  = true
	Boolean HyperEdge_indexed = false
	
	Long HyperEdge_read = 1
	Long HyperEdge_write = 1
	
}