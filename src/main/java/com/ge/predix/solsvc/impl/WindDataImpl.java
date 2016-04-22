package com.ge.predix.solsvc.impl;

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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
	private final long SLEEP_PERIOD_SPRINKLER_OFF = 1000 * 60 *10 ;
	private final long SLEEP_PERIOD_SPRINKLER_ON = 1000 * 10 * 2;
	private final long SLEEP_PERIOD_DAY = 1000 * 60 * 60 * 24;

	/**
	 * -
	 */
	public WindDataImpl() {
		super();
	}

	/**
	 * -
	 */
	@PostConstruct
	public void init() {
		this.serviceManagerService.createRestWebService(this, null);
		//createMetrics();
		while(true){
			call_analytics();
			try {
				Thread.sleep(SLEEP_PERIOD_DAY);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@SuppressWarnings({ "nls", "unchecked" })
	private void call_analytics() {
		System.out.println("*************Analytics *********************");
		String moistureData = getMoistureData();
		//String moistureData = "{\r\n  \"tags\": [\r\n    {\r\n      \"name\": \"Soil-Moisture\",\r\n      \"results\": [\r\n        {\r\n          \"groups\": [\r\n            {\r\n              \"name\": \"type\",\r\n              \"type\": \"number\"\r\n            }\r\n          ],\r\n          \"attributes\": {\r\n            \"Lawn-Type\": [\r\n              \"Cox-Statidium\",\r\n              \"Malony-Field-CS3000\",\r\n              \"Malony-Field-ET2000e\",\r\n              \"West-Campus-Green\"\r\n            ]\r\n          },\r\n          \"values\": [\r\n            [\r\n              1429479623405,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429491296221,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429497287773,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429520487559,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429532416657,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429540945752,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429558997391,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429596812204,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429610829943,\r\n              27,\r\n              3\r\n            ],\r\n            [\r\n              1429641721921,\r\n              27,\r\n              3\r\n            ]\r\n          ]\r\n        }\r\n      ],\r\n      \"stats\": {\r\n        \"rawCount\": 10\r\n      }\r\n    }\r\n  ]\r\n}";
		//String moistureData = "{\"number1\":1,\"number2\":2}";
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
	            double stats = getStatAsInt(resultEntry);
	            insertInTimeSeries(stats);
	            
	            
	           	System.out.println("=========================");
	   			System.out.println(resultEntry.toString());
	   			System.out.println("=========================");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	           
	   		
	        }
		//poll for the results from run analytics
		
		
		
	}
	
	private void insertInTimeSeries(double stats) {
		Body body = new Body();
		body.setName("Statistics");
		
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
		dpIngestion.setBody(bodies);
		this.timeseriesFactory.create(dpIngestion);
		System.out.println("Data Entered");
		
		
	}

	private double getStatAsInt(String resultEntry) {
		// TODO Auto-generated method stub
		String[] splitArr = resultEntry.split(":");
		double stat = Double.parseDouble(splitArr[1].substring(0,splitArr[1].length()-1));
		return stat;
	}

	private String getMoistureData(){
		List<Header> headers = generateHeaders();
		DatapointsQuery dq = buildDatapointsQueryRequest("Soil-Moisture",
				"48h-ago","24h-ago", 10000, "desc");
		DatapointsResponse response = this.timeseriesFactory.queryForDatapoints(this.timeseriesRestConfig.getBaseUrl(),
				dq, headers);
		ObjectMapper mapper = new ObjectMapper();
		String moistureDataJson = "";
		try {
			moistureDataJson = mapper.writeValueAsString(response);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return moistureDataJson;
		//System.out.println(moistureDataJson);
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
	

	private DatapointsLatestQuery buildLatestDatapointsQueryRequest(String id) {
		DatapointsLatestQuery datapointsLatestQuery = new DatapointsLatestQuery();

		com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag tag = new com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag();
		tag.setName(id);
		List<com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag> tags = new ArrayList<com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag>();
		tags.add(tag);
		datapointsLatestQuery.setTags(tags);
		return datapointsLatestQuery;
	}

	/**
	 * 
	 * @param id
	 * @param startDuration
	 * @param tagorder
	 * @return
	 */
	private DatapointsQuery buildDatapointsQueryRequest(String id,
			String startDuration,String endDuration, int taglimit, String tagorder) {
		DatapointsQuery datapointsQuery = new DatapointsQuery();
		List<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag> tags = new ArrayList<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag>();
		datapointsQuery.setStart(startDuration);
		datapointsQuery.setEnd(endDuration);
		
		//datapointsQuery.setStart("1y-ago"); //$NON-NLS-1$
		String[] tagArray = id.split(","); //$NON-NLS-1$
		List<String> entryTags = Arrays.asList(tagArray);

		for (String entryTag : entryTags) {
			com.ge.predix.timeseries.entity.datapoints.queryrequest.Filters filters = new com.ge.predix.timeseries.entity.datapoints.queryrequest.Filters();
			
			List<Attribute> attributes = new ArrayList<Attribute>();
			Attribute attr = new Attribute();
			
			List<String> host = new ArrayList<String>();
			host.add("ON");
			
			attr.setHost(host);
			attributes.add(attr);
			
			filters.setAttributes(attributes);
			
			com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag tag = new com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag();
			tag.setName(entryTag);
			tag.setLimit(taglimit);
			tag.setOrder(tagorder);
			//tag.setFilters(filters);
			tags.add(tag);
		}
		DatapointsQuery datapointsQuery2 = datapointsQuery;
		datapointsQuery2.setTags(tags);
		return datapointsQuery;
	}

	
	
	
	

	private DatapointsIngestion getInsertionData(double moisture, String status) {
		DatapointsIngestion dpIngestion = new DatapointsIngestion();
		dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));

		List<Body> bodies = new ArrayList<Body>();
		bodies.add(getDataPoints("Cox-Statidium", moisture, status));
		bodies.add(getDataPoints("West-Campus-Green", moisture, status));
		bodies.add(getDataPoints("Malony-Field-CS3000", moisture, status));
		bodies.add(getDataPoints("Malony-Field-ET2000e", moisture, status));

		dpIngestion.setBody(bodies);
		return dpIngestion;
	}


	// create data to be inserted in timeSeries
	private Body getDataPoints(String type, double moisture, String status) {
		Body body = new Body();
		body.setName("Soil-Moisture");
		long timestamp = System.currentTimeMillis();
		List<Object> datapoint1 = new ArrayList<Object>();
		datapoint1.add(timestamp);
		datapoint1.add(moisture);
		datapoint1.add(2); // quality

		List<Object> datapoint2 = new ArrayList<Object>();
		datapoint2.add(timestamp);
		datapoint2.add(moisture);
		datapoint2.add(3); // quality

		List<Object> datapoint3 = new ArrayList<Object>();
		datapoint3.add(timestamp);
		datapoint3.add(moisture);
		datapoint3.add(3); // quality

		List<Object> datapoints = new ArrayList<Object>();
		datapoints.add(datapoint1);
		datapoints.add(datapoint2);
		datapoints.add(datapoint3);

		body.setDatapoints(datapoints);
		com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
		map.put("Lawn-Type", type);
		map.put("Status", status);
		System.out.println("Inserting  " + +moisture + " " + status + " "
				+ type);

		body.setAttributes(map);
		return body;
	}

	@SuppressWarnings("javadoc")
	protected Response handleResult(Object entity) {
		ResponseBuilder responseBuilder = Response.status(Status.OK);
		responseBuilder.type(MediaType.APPLICATION_JSON);
		responseBuilder.entity(entity);
		return responseBuilder.build();
	}

	private Long generateTimestampsWithinYear(Long current) {
		long yearInMMS = Long.valueOf(31536000000L);
		return ThreadLocalRandom.current().nextLong(current - yearInMMS,
				current + 1);
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
