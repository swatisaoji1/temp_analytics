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
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ge.predix.solsvc.api.WindDataAPI;
import com.ge.predix.solsvc.restclient.impl.CxfAwareRestClient;
import com.ge.predix.solsvc.spi.IServiceManagerService;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesRestConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesWSConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.factories.TimeseriesFactory;
import com.ge.predix.solsvc.timeseries.bootstrap.websocket.client.TimeseriesWebsocketClient;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.Body;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.DatapointsIngestion;
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
		createMetrics();
	}

	@Override
	public Response greetings() {
		return handleResult("Greetings from CXF Bean Rest Service " + new Date()); //$NON-NLS-1$
	}

	@Override
	public Response getYearlyWindDataPoints(String id, String authorization,String starttime,String taglimit,String tagorder) {
		if (id == null) {
			return null;
		}
		
		List<Header> headers = generateHeaders();

		DatapointsQuery dpQuery = buildDatapointsQueryRequest(id, starttime,getInteger(taglimit),tagorder);
		DatapointsResponse response = this.timeseriesFactory
				.queryForDatapoints(this.timeseriesRestConfig.getBaseUrl(),
						dpQuery, headers);
		log.debug(response.toString());

		return handleResult(response);
	}

	/**
	 * 
	 * @param s -
	 * @return
	 */
	private int getInteger(String s) {
		int inValue = 25;
		try {
			inValue = Integer.parseInt(s);
			
		} catch (NumberFormatException ex) {
			// s is not an integer
		}
		return inValue;
	}

	@Override
	public Response getLatestWindDataPoints(String id, String authorization) {
		if (id == null) {
			return null;
		}
		List<Header> headers = generateHeaders();

		DatapointsLatestQuery dpQuery = buildLatestDatapointsQueryRequest(id);
		DatapointsResponse response = this.timeseriesFactory
				.queryForLatestDatapoint(
						this.timeseriesRestConfig.getBaseUrl(), dpQuery,
						headers);
		log.debug(response.toString());

		return handleResult(response);
	}

	@SuppressWarnings({ "unqualified-field-access", "nls" })
	private List<Header> generateHeaders()
    {
        List<Header> headers = this.restClient.getSecureTokenForClientId();
		this.restClient.addZoneToHeaders(headers,
				this.timeseriesRestConfig.getZoneId());
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
			String startDuration, int taglimit, String tagorder) {
		DatapointsQuery datapointsQuery = new DatapointsQuery();
		List<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag> tags = new ArrayList<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag>();
		datapointsQuery.setStart(startDuration);
		//datapointsQuery.setStart("1y-ago"); //$NON-NLS-1$
		String[] tagArray = id.split(","); //$NON-NLS-1$
		List<String> entryTags = Arrays.asList(tagArray);

		for (String entryTag : entryTags) {
			com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag tag = new com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag();
			tag.setName(entryTag);
			tag.setLimit(taglimit);
			tag.setOrder(tagorder); 
			tags.add(tag);
		}
		datapointsQuery.setTags(tags);
		return datapointsQuery;
	}

	private double getEvaporationRate(){
		//TODO call weather api to get the evaporation rate and
		WeatherRestClient wc = new WeatherRestClient();
		String weatherData = wc.getWetherInformation("San%20Francisco");
		System.out.println(weatherData);
		double temp = 70;
		double humidity = 50;
		double windSpeed = 2;
		try {
			JSONObject json = new JSONObject(weatherData);
			temp = json.getJSONObject("main").getDouble("temp");
			humidity = json.getJSONObject("main").getDouble("humidity");
			windSpeed = json.getJSONObject("wind").getDouble("speed");
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//calculate evaporation per 10 minutes
		System.out.println("temperature : " + temp);
		double hourlyEvaporation = temp * windSpeed /humidity;
		System.out.println("Rate : " + hourlyEvaporation/6);
		// return moisture loss for 10 minutes
		return hourlyEvaporation / 6;
	}
	
	
	
	@SuppressWarnings({ "nls", "unchecked" })
	private void createMetrics() {
		while(true){
			int loopcount = 0;
			double moisture = 100;
			double evaporationRate = getEvaporationRate();
			
			// decrement moisture level 
			while(moisture > 20){
				DatapointsIngestion dpIngestion = new DatapointsIngestion();
				dpIngestion
				.setMessageId(String.valueOf(System.currentTimeMillis()));
							
				List<Body> bodies = new ArrayList<Body>();
			    bodies.add(getDataPoints("Cox-Statidium", moisture));
			    bodies.add(getDataPoints("West-Campus-Green", moisture));
			    bodies.add(getDataPoints("Malony-Field-CS3000", moisture));
			    bodies.add(getDataPoints("Malony-Field-ET2000e", moisture));
				
			   
				dpIngestion.setBody(bodies);
				this.timeseriesFactory.create(dpIngestion);
				
				// sleep for 10 minutes
				try {
					Thread.sleep(1000* 60 * 10);
					log.debug("sleeping for 10 secs ***********" + moisture);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// decrement the moisture
				moisture = moisture - evaporationRate;
			}// end of inner loop one sprinkler cycle
			loopcount++;
			log.debug("loopcount: " + loopcount);
			// TODO : trigger sprinkler
			
			// increment moisture
			while(moisture <= 100){
				DatapointsIngestion dpIngestion = new DatapointsIngestion();
				dpIngestion
				.setMessageId(String.valueOf(System.currentTimeMillis()));
							
				List<Body> bodies = new ArrayList<Body>();
			    bodies.add(getDataPoints("Cox-Statidium", moisture));
			    bodies.add(getDataPoints("West-Campus-Green", moisture));
			    bodies.add(getDataPoints("Malony-Field-CS3000", moisture));
			    bodies.add(getDataPoints("Malony-Field-ET2000e", moisture));
				
			   
				dpIngestion.setBody(bodies);
				this.timeseriesFactory.create(dpIngestion);
				moisture += 4;
				
				// sleep for 1 minute
				try {
					Thread.sleep(1000 * 60);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} // inner while loop
			
		}// endless loop
	}

	// create data to be inserted in timeSeries 
	private Body getDataPoints(String type,double moisture){
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
		map.put("Lawn-Type", type); //$NON-NLS-2$

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

}
