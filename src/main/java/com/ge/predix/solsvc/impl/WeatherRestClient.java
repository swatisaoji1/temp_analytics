package com.ge.predix.solsvc.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * {"id":88319,"dt":1345284000,"name":"Benghazi",
    "coord":{"lat":32.12,"lon":20.07},
    "main":{"temp":306.15,"pressure":1013,"humidity":44,"temp_min":306,"temp_max":306},
    "wind":{"speed":1,"deg":-7},
    "weather":[
                 {"id":520,"main":"Rain","description":"light intensity shower rain","icon":"09d"},
                 {"id":500,"main":"Rain","description":"light rain","icon":"10d"},
                 {"id":701,"main":"Mist","description":"mist","icon":"50d"}
              ],
    "clouds":{"all":90},
    "rain":{"3h":3}}

 */

public class WeatherRestClient {

	final String  WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather";
	final String API_KEY = "4fda58913e5b362e345f684ec484c4d7";		
	private static Logger log = LoggerFactory.getLogger(WeatherRestClient.class);
	
	
	/*public static void main(String[] args) {
		WeatherRestClient client = new WeatherRestClient();
		String response = client.getWetherInformation("San%20Francisco");
		JSONObject obj = new JSONObject(response);
			}*/

	private String getURL(String city) {
		StringBuilder builder = new StringBuilder();
		builder.append(WEATHER_URL);
		builder.append("?");
		builder.append("APPID=" + API_KEY);
		builder.append("&q=" + city);
		System.out.println(builder.toString());
		return builder.toString();
	}
	
	// method to return weather information by city
	public  String getWetherInformation(String city) {

		try {

			URL url = new URL(getURL(city));
			// Open a new connection for the passed URL
			HttpURLConnection conn = null;
			conn = (HttpURLConnection) url.openConnection();

			// Setting timeout to 5 sec
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(5000);

			// if REST call is not successful, throw an exception
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));
			StringBuilder sb = new StringBuilder();
			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				sb.append(output);
				// System.out.println(output);

			}
			conn.disconnect();
			return sb.toString();
		} catch (ProtocolException e) {
			log.error(e.getMessage() + e.getStackTrace());
			e.printStackTrace();
		} catch (MalformedURLException e) {
			log.error(e.getMessage() + e.getStackTrace());
			e.printStackTrace();
		} catch (IOException e) {
			log.error(e.getMessage() + e.getStackTrace());
			e.printStackTrace();
		}
		return null;
	}
}
