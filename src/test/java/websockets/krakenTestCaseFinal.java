package websockets;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class krakenTestCaseFinal {
	@Test
	void validateInvalidDepthErrorForBook() {
		// Given book subscription message with invalid depth
		String publicWebSocketSubscriptionMsg = "{ \"event\":\"subscribe\", \"subscription\":{\"depth\": 2,\"name\":\"book\"},\"pair\":[\"XBT/USD\"] }";
		

		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 2);

		// Then error  		
		String expectedErrorMsg = "Subscription depth not supported";

		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
				
				Assert.assertEquals(expectedErrorMsg, jsonobject.getString("errorMessage"));
				Assert.assertEquals("error", jsonobject.getString("status"));
			}
		}		
	}
	
	@Test
	void validateInvalidIntervalErrorForOhlc() {
		// Given ohlc subscription message with invalid interval
		String publicWebSocketSubscriptionMsg = "{ \"event\": \"subscribe\", \"subscription\": { \"interval\": 1441, \"name\": \"ohlc\"}, \"pair\": [ \"XBT/EUR\" ]}";

		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 2);

		// Then error  		
		String expectedErrorMsg = "Subscription ohlc interval not supported";

		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
				
				Assert.assertEquals(expectedErrorMsg, jsonobject.getString("errorMessage"));
				Assert.assertEquals("error", jsonobject.getString("status"));
			}
		}		
	}
	
	@Test
	void validateInvalidPairErrorForTicker() {
		// Given ticker subscription message with invalid pair
		String publicWebSocketSubscriptionMsg = "{ \"event\": \"subscribe\", \"pair\": [ \"ABC/DEF\" ], \"subscription\": { \"name\": \"ticker\" } }";
		
		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 2);

		// Then error  		
		String expectedErrorMsg = "Currency pair not supported ABC/DEF";
		
		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
				
				Assert.assertEquals(expectedErrorMsg, jsonobject.getString("errorMessage"));
				Assert.assertEquals("error", jsonobject.getString("status"));
			}
		}
	}
	
	@Test
	void validateOrderIsNotCrossedInOrderBook() {
		String publicWebSocketSubscriptionMsg = "{ \"event\":\"subscribe\", \"subscription\":{\"name\":\"book\"},\"pair\":[\"XBT/USD\"] }";
		
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 10);
				
		for(int i=0; i<payLoadMsg.size(); i++) {
			Map.Entry<LocalDateTime,String> entry = payLoadMsg.get(i).entrySet().iterator().next();
			
			if(entry.getValue().contains("as")) {
				JSONArray jsonarray = new JSONArray(entry.getValue());
				JSONObject jsonobject = jsonarray.getJSONObject(1);
				
				JSONArray jsonNestedArrayAs = jsonobject.getJSONArray("as");
				
				List<Double> askList = convertJSONtoList(jsonNestedArrayAs.get(0).toString());
				List<Double> bidList = convertJSONtoList(jsonobject.getJSONArray("bs").get(0).toString());
				
				Assert.assertTrue(askList.get(0).doubleValue() > bidList.get(0).doubleValue());
			}
		}		
			
	}
	
	@Test
	void validateTickerSubscriptionStatus() {
		// Given valid ticker subscription message
		String publicWebSocketSubscriptionMsg = "{ \"event\": \"subscribe\", \"pair\": [ \"XBT/USD\"], \"subscription\": { \"name\": \"ticker\" } }";

		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 5);
				
		// Then Subscription Status is correctly sent
		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
						
				Assert.assertEquals("subscriptionStatus", jsonobject.getString("event"));
				Assert.assertEquals("ticker", jsonobject.getString("channelName"));
				Assert.assertEquals("XBT/USD", jsonobject.getString("pair"));
				Assert.assertEquals("subscribed", jsonobject.getString("status"));
				
				JSONObject jsonobjectSubscription = jsonobject.getJSONObject("subscription");
				Assert.assertEquals("ticker", jsonobjectSubscription.getString("name"));
			}
		}	
	}
	
	@Test
	void validateBookSubscriptionStatus() {
		// Given valid book subscription message
		String publicWebSocketSubscriptionMsg = "{ \"event\":\"subscribe\", \"subscription\":{\"name\":\"book\"},\"pair\":[\"XBT/USD\"] }";
		
		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 5);
				
		// Then Subscription Status is correctly sent
		for(int i=0; i<payLoadMsg.size(); i++) {
			Map.Entry<LocalDateTime,String> entry = payLoadMsg.get(i).entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
						
				Assert.assertEquals("subscriptionStatus", jsonobject.getString("event"));
				Assert.assertEquals("book-10", jsonobject.getString("channelName"));
				Assert.assertEquals("XBT/USD", jsonobject.getString("pair"));
				Assert.assertEquals("subscribed", jsonobject.getString("status"));
			
				JSONObject jsonobjectSubscription = jsonobject.getJSONObject("subscription");
				Assert.assertEquals(10, jsonobjectSubscription.get("depth"));
				Assert.assertEquals("book", jsonobjectSubscription.getString("name"));
			}
		}
	}
	
	@Test
	void validateOhlcSubscriptionStatus() {
		// Given valid ohlc subscription message
		String publicWebSocketSubscriptionMsg = "{ \"event\": \"subscribe\", \"subscription\": { \"interval\": 1440, \"name\": \"ohlc\"}, \"pair\": [ \"XBT/EUR\" ]}";
		
		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 5);
		
		// Then Subscription Status is correctly sent
		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
				
				Assert.assertEquals("subscriptionStatus", jsonobject.getString("event"));
				Assert.assertEquals("ohlc-1440", jsonobject.getString("channelName"));
				Assert.assertEquals("XBT/EUR", jsonobject.getString("pair"));
				Assert.assertEquals("subscribed", jsonobject.getString("status"));
				
				JSONObject jsonobjectSubscription = jsonobject.getJSONObject("subscription");
				Assert.assertEquals(1440, jsonobjectSubscription.get("interval"));
				Assert.assertEquals("ohlc", jsonobjectSubscription.getString("name"));
			}
		}
		
	}
	
	@Test
	void validateDepthValuesOrderSubscription() {
		// Given array of valid depth values for book
		ArrayList<Integer> validDepthValues = new ArrayList<>(List.of(10, 25, 100, 500, 1000));
		
		String publicWebSocketSubscriptionMsg = new String();
		
		// Then 
		for(int depth : validDepthValues) {
			publicWebSocketSubscriptionMsg = "{ \"event\":\"subscribe\", \"subscription\":{\"depth\": " + depth + ",\"name\":\"book\"},\"pair\":[\"XBT/USD\"] }";
			ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 2);
			
			for(int i=0; i<payLoadMsg.size(); i++) { 
				Map.Entry<LocalDateTime,String> entry = payLoadMsg.get(i).entrySet().iterator().next();
				
				if(entry.getValue().charAt(0) == '{') {
					JSONObject jsonobject = new JSONObject(entry.getValue());
					Assert.assertNotEquals("error", jsonobject.getString("status"));
				}

			}
		}
				
	}
	
	@Test
	void validateInvalidSubscriptionMessage() {
		// Given subscription message is missing name
		String publicWebSocketSubscriptionMsg = "{ \"event\":\"subscribe\", \"pair\":[\"XBT/USD\"] }";
		

		// When connection is created
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 2);

		// Then error  		
		String expectedErrorMsg = "Subscription(s) not found";

		for(HashMap<LocalDateTime, String> hm : payLoadMsg) {
			Map.Entry<LocalDateTime,String> entry = hm.entrySet().iterator().next();
		
			if(entry.getValue().contains("subscriptionStatus")) {
				JSONObject jsonobject = new JSONObject(entry.getValue());
				
				Assert.assertEquals(expectedErrorMsg, jsonobject.getString("errorMessage"));
				Assert.assertEquals("error", jsonobject.getString("status"));
			}
		}		

	}
	
	@Test
	void validateTimeIncreasesOverFeed() {
		// Given subscription message to ticker channel
		String publicWebSocketSubscriptionMsg = "{ \"event\": \"subscribe\", \"pair\": [ \"XBT/USD\", \"XBT/EUR\" ], \"subscription\": { \"name\": \"ticker\" } }";
		
		// When connection is established
		ArrayList<HashMap<LocalDateTime, String>> payLoadMsg = createConnection(publicWebSocketSubscriptionMsg, 10);
		
		// Then time increases over feed
		for(int i=0; i<payLoadMsg.size()-1; i++) {
			Map.Entry<LocalDateTime,String> entry = payLoadMsg.get(i).entrySet().iterator().next();
			Map.Entry<LocalDateTime,String> nextEntry = payLoadMsg.get(i+1).entrySet().iterator().next();
			
			LocalDateTime previousEntry = entry.getKey();

			LocalDateTime followingEntry = nextEntry.getKey();
			Assert.assertTrue(previousEntry.isBefore(followingEntry));
			
		}
		
	}

	private ArrayList<HashMap<LocalDateTime, String>> createConnection(String publicWebSocketSubscriptionMsg, int count) {
		String publicWebSocketURL = "wss://ws.kraken.com/";
		
		return OpenAndStreamWebSocketSubscription(publicWebSocketURL, publicWebSocketSubscriptionMsg, count);

	}
	
	public static ArrayList<HashMap<LocalDateTime, String>> OpenAndStreamWebSocketSubscription(String connectionURL, String webSocketSubscription, int count) {
		CountDownLatch latch = new CountDownLatch(count);
		WebSocketClient webSocketClient = new WebSocketClient(latch);
		
		
        try {
            WebSocket ws = HttpClient
                    .newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(connectionURL), webSocketClient)
                    .join();
            ws.sendText(webSocketSubscription, true);
   
            latch.await();

        } catch (

        Exception e) {
            System.out.println();
            System.out.println("AN EXCEPTION OCCURED :(");
            System.out.println(e);
        }
        return webSocketClient.payloadResponse;
    }
	

    private static class WebSocketClient implements WebSocket.Listener {
        private final CountDownLatch latch;
        public ArrayList<HashMap<LocalDateTime, String>> payloadResponse = new ArrayList<HashMap<LocalDateTime, String>>();
        
        public WebSocketClient(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            
            HashMap<LocalDateTime, String> data1 = new HashMap<>();
            data1.put(LocalDateTime.now(), data.toString());
            payloadResponse.add(data1);
            latch.countDown();
            return WebSocket.Listener.super.onText(webSocket, data, false);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            System.out.println("ERROR OCCURED: " + webSocket.toString());
            WebSocket.Listener.super.onError(webSocket, error);
        }
    }
    
    
    private List<Double> convertJSONtoList(String jsonStr) {
    	  ObjectMapper objectMapper = new ObjectMapper();
	      List<String> jsonToList = new ArrayList<String>();
	      try {
	    	  jsonToList = objectMapper.readValue(jsonStr, List.class);
	      } catch(Exception e) {
	         e.printStackTrace();
	      }
	      return jsonToList.stream().map(Double::valueOf).collect(Collectors.toList());
    }
}
