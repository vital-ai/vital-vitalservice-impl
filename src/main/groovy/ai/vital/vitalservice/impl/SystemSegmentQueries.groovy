package ai.vital.vitalservice.impl

import ai.vital.query.Capture;
import ai.vital.query.Direction;
import ai.vital.query.Utils;
import ai.vital.query.querybuilder.VitalBuilder;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.Edge_hasApp;
import ai.vital.vitalsigns.model.Edge_hasAuthKey;
import ai.vital.vitalsigns.model.Edge_hasProvisioning;
import ai.vital.vitalsigns.model.Edge_hasSegment;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalAuthKey;
import ai.vital.vitalsigns.model.VitalAuthKey_PropertiesHelper;
import ai.vital.vitalsigns.model.VitalOrganization;
import ai.vital.vitalsigns.model.VitalProvisioning;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceRootKey;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.query.URI

class SystemSegmentQueries {

	static VitalBuilder builder = new VitalBuilder()

	public static VitalSelectQuery getOrganizationQuery(VitalSegment systemSegment, String organizationID) {

		return builder.query {
			
			SELECT {
				
				value segments: [systemSegment]
				
				node_constraint { VitalOrganization.props().organizationID.equalTo(organizationID) }
				
				node_constraint { VitalOrganization.class }
				
			}
			
		}.toQuery()
	
	}

	public static VitalSelectQuery getOrganizationsListQuery(
			VitalSegment systemSegment) {
			
		return builder.query {
			
			SELECT {
				
				value segments: [systemSegment]
				
				node_constraint { VitalOrganization.class }
				
			}
			
		}.toQuery()
	}

	public static VitalGraphQuery getAppsQuery(VitalSegment systemSegment,
			VitalOrganization organization) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
					
					node_constraint { VitalOrganization.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(organization.getURI()) ) }
					
					ARC {
						
						edge_constraint { Edge_hasApp.class }
						
						node_constraint { VitalApp.class }
						
					}
					
				}
				
			}
			
		}.toQuery()
	}

	public static VitalGraphQuery getSegmentsQuery(VitalSegment systemSegment,
			VitalOrganization organization, VitalApp app) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
					
					node_constraint { VitalApp.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(app.getURI()) ) }
					
					value capture: Capture.NONE
					
					
//					if(includeKeys) {
//						
//						ARC_OR {
//						
//							ARC {
//								
//								value capture: Capture.BOTH
//								
//								edge_constraint { Edge_hasSegment.class }
//								
//								node_constraint { VitalSegment.class }
//								
//								
//								ARC {
//									
//									value capture: Capture.BOTH
//									
//									value optional: true
//									
//									edge_constraint { Edge_hasProvisioning.class }
//									
//									node_constraint { VitalProvisioning.expandSubclasses(true) }
//									
//								}
//								
//							}
//							
//							ARC {
//								
//								edge_constraint { Edge_hasAuthKey.class }
//								
//								node_costraint { VitalServiceKey.class }
//								
//							}
//						}
//						
//					} else {
					
						ARC {
							
							value capture: Capture.BOTH
							
							edge_constraint { Edge_hasSegment.class }
							
							node_constraint { VitalSegment.class }
							
							
							ARC {
								
								value capture: Capture.BOTH
								
								value optional: true
								
								edge_constraint { Edge_hasProvisioning.class }
								
								node_constraint { VitalProvisioning.expandSubclasses(true) }
								
							}
							
						}
						
//					}
					
				}
				
			}
			
		}.toQuery()
	}

	
	public static VitalGraphQuery getFullAppObject(VitalSegment systemSegment,
		VitalOrganization organization, VitalApp app) {
		
	return builder.query {
		
		GRAPH {
			
			value inlineObjects: true
			
			value segments: [systemSegment]
			
			ARC {
				
				node_constraint { VitalApp.class }
			
				node_constraint { URI.equalTo( URIProperty.withString(app.getURI()) ) }
				
				value capture: Capture.NONE
				
				ARC_OR {

					ARC {

						value capture: Capture.BOTH

						edge_constraint { Edge_hasSegment.class }

						node_constraint { VitalSegment.class }


						ARC {

							value capture: Capture.BOTH

							value optional: true

							edge_constraint { Edge_hasProvisioning.class }

							node_constraint { VitalProvisioning.expandSubclasses(true) }

						}

					}

					ARC {

						edge_constraint { Edge_hasAuthKey.class }

						node_constraint { VitalServiceKey.class }

					}
				}

			}
			
		}
		
	}.toQuery()
}

	public static VitalGraphQuery getSegmentQuery(VitalSegment systemSegment,
		VitalOrganization organization, VitalApp app, VitalSegment optionalFilter) {
		
	return builder.query {
		
		GRAPH {
			
			value inlineObjects: true
			
			value segments: [systemSegment]
			
			ARC {
				
				node_constraint { VitalApp.class }
			
				node_constraint { URI.equalTo( URIProperty.withString(app.getURI()) ) }
				
				value capture: Capture.NONE
				
				ARC {
					
					value capture: Capture.BOTH
					
					edge_constraint { Edge_hasSegment.class }
						
//						node_constraint { VitalSegment.class }
						
					node_constraint { URI.equalTo( URIProperty.withString(optionalFilter.getURI()) ) }
//					node_constraint { Utils.PropertyConstraint(Property_hasSegmentID.class).equalTo(optionalFilter.segmentID.toString()) }
						
					
					
					ARC {
						
						value capture: Capture.BOTH
						
						value optional: true
						
						edge_constraint { Edge_hasProvisioning.class }
						
						node_constraint { VitalProvisioning.expandSubclasses(true) }
						
					}
					
				}
				
			}
			
		}
		
	}.toQuery()
}

	public static VitalGraphQuery getRootAndAdminKeysQuery(VitalSegment systemSegment,
			VitalOrganization organization) {

		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
					
					node_constraint { VitalOrganization.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(organization.getURI()) ) }
					
					ARC {
						
						edge_constraint { Edge_hasAuthKey.class }
						
						node_constraint { VitalAuthKey.expandSubclasses(true) }
						
					}
					
				}
				
			}
			
		}.toQuery()
			
	}

	public static VitalGraphQuery getAllSegmentsQuery(VitalSegment systemSegment,
			VitalOrganization organization) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
					
					node_constraint { VitalOrganization.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(organization.getURI()) ) }
					
					ARC {
						
						edge_constraint { Edge_hasApp.class }
						
						node_constraint { VitalApp.class }
						
						ARC {
						
							edge_constraint { Edge_hasSegment.class }
							
							node_constraint { VitalSegment.class }
							
							
							ARC {
								
								value optional: true
								
								edge_constraint { Edge_hasProvisioning.class }
								
								node_constraint { VitalProvisioning.expandSubclasses(true) }
								
							}
							
						}
						
					}
					
				}
				
			}
			
		}.toQuery()
	}

	public static VitalGraphQuery getAllSegmentsQueryGlobal(
			VitalSegment systemSegment) {

		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					edge_constraint { Edge_hasSegment.class }
					
					node_constraint { VitalSegment.class }
					
					
					ARC {
						
						value optional: true
						
						edge_constraint { Edge_hasProvisioning.class }
						
						node_constraint { VitalProvisioning.expandSubclasses(true) }
						
					}
					
				}
						
			}
			
		}.toQuery()

	}

	public static VitalGraphQuery getAppServiceKeys(VitalSegment systemSegment,
			VitalApp app2) {

		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					node_constraint { VitalApp.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(app2.getURI()) ) }
					
					
					ARC {
						
						edge_constraint { Edge_hasAuthKey.class }
						
						node_constraint { VitalServiceKey.class }
						
					}
					
				}
						
			}
			
		}.toQuery()
	}

	public static VitalGraphQuery getOrganizationServiceAdminKeys(
			VitalSegment systemSegment, VitalOrganization organization) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					node_constraint { VitalOrganization.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(organization.getURI()) ) }
					
					
					ARC {
						
						edge_constraint { Edge_hasAuthKey.class }
						
						node_constraint { VitalServiceAdminKey.class }
						
					}
					
				}
						
			}
			
		}.toQuery()
	}

	public static VitalSelectQuery getRootKeysQuery(VitalSegment systemSegment) {

		return builder.query {
			
			SELECT {
				
				value segments: [systemSegment]
				
				node_constraint { VitalServiceRootKey.class }
				
			}
			
		}.toQuery()

	}

	public static VitalGraphQuery getOrganizationToRootKeyQuery(
			VitalSegment systemSegment, VitalOrganization organization) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					node_constraint { VitalOrganization.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(organization.getURI()) ) }
					
					ARC_OR {
					
						ARC {
							
							value direction: 'reverse'
							
							edge_constraint { Edge_hasAuthKey.class }
							
							node_constraint { VitalServiceRootKey.class }
							
						}
					
						ARC {
							
							edge_constraint { Edge_hasAuthKey.class }
							
							node_constraint { VitalServiceAdminKey.class }
							
						}
					}					
					
				}
						
			}
			
		}.toQuery()
	}

	public static VitalSelectQuery getAuthKeyQuery(VitalSegment systemSegment,
			String key) {
			
		return builder.query {
			
			SELECT {
				
				value segments: [systemSegment]
				
				node_constraint { VitalAuthKey.class.expandSubclasses(true) }
				
				node_constraint { ((VitalAuthKey_PropertiesHelper)VitalAuthKey.props()).key.equalTo(key) }
				
			}
			
		}.toQuery()
	
	}

	public static VitalGraphQuery getServiceKeyAppOrg(
			VitalSegment systemSegment, VitalServiceKey serviceKey) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					node_constraint { VitalServiceKey.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(serviceKey.getURI()) ) }
					
					ARC {
							
						value direction: 'reverse'
							
						edge_constraint { Edge_hasAuthKey.class }
						
						node_constraint { VitalApp.class }
							
						ARC {
							
							value direction: 'reverse'
							
							edge_constraint { Edge_hasApp.class }
							
							node_constraint { VitalOrganization.class }
							
						}
						
					}
					
				}
						
			}
			
		}.toQuery()
	}

	public static VitalGraphQuery getServiceAdminKeyOrg(
			VitalSegment systemSegment, VitalServiceAdminKey serviceAdminKey) {
			
		return builder.query {
			
			GRAPH {
				
				value inlineObjects: true
				
				value segments: [systemSegment]
				
				ARC {
				
					node_constraint { VitalServiceAdminKey.class }
				
					node_constraint { URI.equalTo( URIProperty.withString(serviceAdminKey.getURI()) ) }
					
					ARC {
							
						value direction: 'reverse'
							
						edge_constraint { Edge_hasAuthKey.class }
						
						node_constraint { VitalOrganization.class }
							
						
					}
					
				}
						
			}
			
		}.toQuery()
	}
	
}
