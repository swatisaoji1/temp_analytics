package com.ge.predix.solsvc.impl;

import java.util.List;

import org.apache.http.Header;

public class AnalyticAPI {
	private final String ANALYTIC_ZONE_ID = "491636b4-4769-4863-8999-1fc0a3498e60";
	private final String CATALOG_ENTRY_ID = "7805917d-71cf-489f-b1d0-2a14ebfe393b";
	//private final String CATALOG_ENTRY_ID = "f9f2194c-0951-4d92-a24e-a415424991b3";
	private final String baseURI = "https://predix-analytics-catalog-release.run.aws-usw02-pr.ice.predix.io";
	
	
	
	
	
	
	public String getListOfAnalyticsURI(){
		//https://predix-analytics-catalog-release.run.aws-usw02-pr.ice.predix.io/api/v1/catalog/analytics
		return (baseURI + "/api/v1/catalog/analytics");	
	}
	
	public String getRunAnalyticURI(){
		//https://predix-analytics-catalog-release.run.aws-usw02-pr.ice.predix.io/api/v1/catalog/analytics/<analyticCatalogEntryId-goes-here>/validation
		return (baseURI + "/api/v1/catalog/analytics/" + CATALOG_ENTRY_ID + "/execution");	
	}
	
	
	
	
	/**
	 * @return the baseURI
	 */
	public String getBaseURI() {
		return baseURI;
	}

	/**
	 * @return the aNALYTIC_ZONE_ID
	 */
	public String getANALYTIC_ZONE_ID() {
		return ANALYTIC_ZONE_ID;
	}
	/**
	 * @return the cATALOG_ENTRY_ID
	 */
	public String getCATALOG_ENTRY_ID() {
		return CATALOG_ENTRY_ID;
	}
	
	
}
