package com.ge.predix.solsvc.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.spi.Filter;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import scala.annotation.meta.getter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.predix.solsvc.api.WindDataAPI;
import com.ge.predix.solsvc.restclient.impl.CxfAwareRestClient;
import com.ge.predix.solsvc.spi.IServiceManagerService;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesRestConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesWSConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.factories.TimeseriesFactory;
import com.ge.predix.solsvc.timeseries.bootstrap.websocket.client.TimeseriesWebsocketClient;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.Body;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.DatapointsIngestion;
import com.ge.predix.timeseries.entity.datapoints.queryrequest.Attribute;
import com.ge.predix.timeseries.entity.datapoints.queryrequest.DatapointsQuery;
import com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.DatapointsLatestQuery;
import com.ge.predix.timeseries.entity.datapoints.queryresponse.DatapointsResponse;

/**
 * 
 * @author predix -
 */
@Component
public class WindDataImpl implements WindDataAPI {

	@Autowired
	private IServiceManagerService serviceManagerService;

	@Autowired
	private TimeseriesRestConfig timeseriesRestConfig;

	@Autowired
	private CxfAwareRestClient restClient;

	@Autowired
	private TimeseriesWSConfig tsInjectionWSConfig;

	@Autowired
	private TimeseriesWebsocketClient timeseriesWebsocketClient;

	@Autowired
	private TimeseriesFactory timeseriesFactory;

	private static Logger log = LoggerFactory.getLogger(WindDataImpl.class);
	
	
	private AnalyticAPI aAPI = new AnalyticAPI();
	private final long SLEEP_PERIOD_DAY = 1000 * 60 * 60 * 24;

	/**
	 * -
	 */
	public WindDataImpl() {
		super();
	}

	/**
	 * 
	 */
	@PostConstruct
	public void init() {
		this.serviceManagerService.createRestWebService(this, null);
		while(true){
			
			call_analytics("Cox-Statidium");
			call_analytics("Malony-Field-CS3000");
			call_analytics("Malony-Field-ET2000e");
			call_analytics("West-Campus-Green");
			try {
				Thread.sleep(SLEEP_PERIOD_DAY);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

	@SuppressWarnings({ "nls", "unchecked" })
	private void call_analytics(String lawnType) {
	
		String moistureData  = getMoistureTimeSeriesData(lawnType);
		System.out.println(moistureData);
		
		 
		List<Header> headers = generateAnalyticHeaders();
		System.out.println(headers.toString());
		
		String url = this.aAPI.getRunAnalyticURI();
		System.out.println(url);
		CloseableHttpResponse response = this.restClient.post(url, moistureData, headers);
		HttpEntity responseEntity = response.getEntity();
		
		if (responseEntity != null) {
	           String retSrc;
				try {
					retSrc = EntityUtils.toString(responseEntity);
					// parsing JSON
		            JSONObject result = new JSONObject(retSrc); //Convert String to JSON Object
		            System.out.println(result.toString());
		            String resultEntry = result.getString("result");
		            JSONObject obj = new JSONObject(resultEntry);
		            JSONObject stats = obj.getJSONObject("statistics");
		            double duration = stats.getDouble("duration");
		            double waterUsed = stats.getDouble("waterUsed");
		            insertInTimeSeries(duration, lawnType, "duration");
		            insertInTimeSeries(waterUsed, lawnType, "waterUsed");
		            
		            
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				} 
	           
	   		
	        }
		
	}
	
	
	private String getMoistureTimeSeriesData(String lawnType){
		List<Header> headers = generateHeaders();
		String url = this.timeseriesRestConfig.getBaseUrl() + "/v1/datapoints" ;
		String body = "{\"start\":\"24h-ago\",\"tags\": [{\"name\": \"Soil-Moisture\", \"limit\": 25, \"filters\": {\"attributes\": {\"Lawn-Type\": \"" 
					+ lawnType 
					+ "\", \"Status\": \"ON\"}}}]}";
		
		CloseableHttpResponse response = this.restClient.post(url, body, headers);
		HttpEntity responseEntity = response.getEntity();
		String moistureData = "";
		 if (responseEntity != null) {
	           String retSrc;
			try {
				retSrc = EntityUtils.toString(responseEntity);
				// parsing JSON
	            JSONObject result = new JSONObject(retSrc); //Convert String to JSON Object
	            moistureData = result.toString();         
	            
			} catch (ParseException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			} 	           
		 }
		return moistureData;
		
		
	}
	
	@SuppressWarnings("unchecked")
	private void insertInTimeSeries(double stats, String lawnType, String statType) {
		Body body = new Body();
		body.setName(statType);
		
		//create datapoint
		long timestamp = System.currentTimeMillis();
		List<Object> datapoint1 = new ArrayList<Object>();
		datapoint1.add(timestamp);
		datapoint1.add(stats);
		datapoint1.add(2); // quality
		
		List<Object> datapoints = new ArrayList<Object>();
		datapoints.add(datapoint1);
		body.setDatapoints(datapoints);
		
		DatapointsIngestion dpIngestion = new DatapointsIngestion();
		dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));

		List<Body> bodies = new ArrayList<Body>();
		bodies.add(body);
		
		com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
		map.put("Lawn-Type", lawnType);
		body.setAttributes(map);
				
		dpIngestion.setBody(bodies);
		this.timeseriesFactory.create(dpIngestion);
		System.out.println("Data Entered");
		
		
	}

	private double getStatAsInt(String resultEntry) {
		String[] splitArr = resultEntry.split(":");
		double stat = Double.parseDouble(splitArr[1].substring(0,splitArr[1].length()-1));
		return stat;
	}



	@SuppressWarnings({ "unqualified-field-access", "nls" })
	private List<Header> generateHeaders() {
		List<Header> headers = this.restClient.getSecureTokenForClientId();
		String zoneId = this.timeseriesRestConfig.getZoneId();
		
		
		this.restClient.addZoneToHeaders(headers,
				this.timeseriesRestConfig.getZoneId());
		return headers;
	}
	
	@SuppressWarnings({ "unqualified-field-access", "nls" })
	private List<Header> generateAnalyticHeaders() {
		List<Header> headers = this.restClient.getSecureTokenForClientId();
		Header contentType = new BasicHeader("content-type", "application/json");
		headers.add(contentType);
		String zoneId = aAPI.getANALYTIC_ZONE_ID();
		this.restClient.addZoneToHeaders(headers,zoneId);
		return headers;
	}
	

	@SuppressWarnings("javadoc")
	protected Response handleResult(Object entity) {
		ResponseBuilder responseBuilder = Response.status(Status.OK);
		responseBuilder.type(MediaType.APPLICATION_JSON);
		responseBuilder.entity(entity);
		return responseBuilder.build();
	}

	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ge.predix.solsvc.api.WindDataAPI#getWindDataTags()
	 */
	@Override
	public Response getWindDataTags() {
		List<Header> headers = generateHeaders();
		CloseableHttpResponse httpResponse = null;
		String entity = null;
		try {
			httpResponse = this.restClient
					.get(this.timeseriesRestConfig.getBaseUrl() + "/v1/tags", headers); //$NON-NLS-1$

			if (httpResponse.getEntity() != null) {
				try {
					entity = processHttpResponseEntity(httpResponse.getEntity());
					log.debug("HttpEntity returned from Tags" + httpResponse.getEntity().toString()); //$NON-NLS-1$
				} catch (IOException e) {
					throw new RuntimeException(
							"Error occured calling=" + this.timeseriesRestConfig.getBaseUrl() + "/v1/tags", e); //$NON-NLS-1$//$NON-NLS-2$
				}
			}
		} finally {
			if (httpResponse != null)
				try {
					httpResponse.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
		}

		return handleResult(entity);
	}

	/**
	 * 
	 * @param entity
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("nls")
	private String processHttpResponseEntity(org.apache.http.HttpEntity entity)
			throws IOException {
		if (entity == null)
			return null;
		if (entity instanceof GzipDecompressingEntity) {
			return IOUtils.toString(
					((GzipDecompressingEntity) entity).getContent(), "UTF-8");
		}
		return EntityUtils.toString(entity);
	}



	@Override
	public Response greetings() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public Response getYearlyWindDataPoints(String id, String authorization,
			String starttime, String tagLimit, String tagorder) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public Response getLatestWindDataPoints(String id, String authorization) {
		// TODO Auto-generated method stub
		return null;
	}

}
