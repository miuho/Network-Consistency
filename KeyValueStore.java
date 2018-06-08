import java.util.Queue;
import java.util.LinkedList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	private HashMap<String, ArrayList<StoreValue>> store = null;

	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
	}
	
	// time queue for maintaining get request order
	HashMap<String, TreeMap<String, String>> timeQueues = new HashMap<String, TreeMap<String, String>>();
	// lock for the hashtable
	private final Object lock = new Object();
	
	// add the new request to hashtable
	private void add_to_queue(String key, String time, String value) {
		synchronized (lock) {
			if (timeQueues.get(key) == null) {
				// initialize the sorted TreeMap if the key is new
				TreeMap t = new TreeMap();

				t.put(time, value);
				timeQueues.put(key, t);	
			}
			else {
				// insert to the sorted TreeMap
				timeQueues.get(key).put(time, value);
			}
		}
	}
	
	// signals other waiting threads that this key just finished processing
	private void remove_from_queue(String key, String time) {
		synchronized (lock) {
			// remove the key from hashtable
			timeQueues.get(key).remove(time);

			// wake up waiting threads
			try {lock.notifyAll();} catch (Exception e) {}
		}
	}
	
	// blocks until the key is ready to be processed
	private void wait_until_free(String key, String time, String flag) {
		while (true) {
			synchronized (lock) {
				// make sures that this key is the first in order (time)
				if (timeQueues.get(key) == null ||
					timeQueues.get(key).isEmpty() ||
					Integer.parseInt(time) < Integer.parseInt(timeQueues.get(key).firstKey())) {
					return;
				}
				// wait for another thread to wake up 
				try {lock.wait();} catch (Exception e) {}
			}
		}
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();

		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();

				final String key = map.get("key");
				final String value = map.get("value");
				final String consistency = map.get("consistency");

				Integer region = Integer.parseInt(map.get("region"));

				Long timestamp = Long.parseLong(map.get("timestamp"));

				// fix timestamp
				final Long adjusted = new Long(Skews.handleSkew(timestamp.longValue(), region.intValue()));
				final StoreValue sv = new StoreValue(adjusted, value);

				// no need to use threading for causal and eventual
				if (!consistency.equals("strong")) {
					ArrayList<StoreValue> values = keyValueStore.store.get(key);

					if (values != null) {
						if (consistency.equals("eventual")) {
							// append randomly because order does not matter
							values.add(sv);
						}
						else {
							int i;
							for (i = 0; i < values.size(); i++) {
								// insert into list in a sorted manner
								if (adjusted.longValue() < values.get(i).getTimestamp()) {
									break;
								}
							}
							values.add(i, sv);	
						}
					}

					else {
						// first element in the list
						ArrayList<StoreValue> a = new ArrayList();
						a.add(sv);
						keyValueStore.store.put(key, a);
					}

					String response = "stored";
					req.response().putHeader("Content-Type", "text/plain");
					req.response().putHeader("Content-Length",
							String.valueOf(response.length()));
					req.response().end(response);
					req.response().close();
					return;
				}				
				
				// handle strong consistency
				Thread t = new Thread(new Runnable() {
				public void run() {
					ArrayList<StoreValue> values = keyValueStore.store.get(key);
					if (values != null) {
						int i = 0;
						for (i = 0; i < values.size(); i++) {
							// maintain the sorted order
							if (adjusted.longValue() < values.get(i).getTimestamp()) {
								break;
							}
						}
						values.add(i, sv);	
					}

					else {
						// first element in the list
						ArrayList<StoreValue> a = new ArrayList();

						a.add(sv);
						keyValueStore.store.put(key, a);
					}
				
					String response = "stored";
					req.response().putHeader("Content-Type", "text/plain");
					req.response().putHeader("Content-Length",
							String.valueOf(response.length()));
					req.response().end(response);
					req.response().close();
				}});
				t.start();
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");

				String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				
				// no need to use threading for causal and eventual
				if (!consistency.equals("strong")) {
					String response = "";
					ArrayList<StoreValue> values = keyValueStore.store.get(key);

					// find the list corresponds to the key
					if (values != null) {
						for (StoreValue val : values) {
							response = response + val.getValue() + " ";
						}
					}

					req.response().putHeader("Content-Type", "text/plain");
					if (response != null)
						req.response().putHeader("Content-Length",
								String.valueOf(response.length()));
					req.response().end(response);
					req.response().close();
					return;
				}

				// handle strong consistency
				Thread t = new Thread(new Runnable() {
				public void run() {
					String response = "";

					// wait for the all put operations (among all data centers) to complete before reading
					wait_until_free(key, timestamp.toString(), "");
					ArrayList<StoreValue> values = keyValueStore.store.get(key);

					// find the corresponding list for the key
					if (values != null) {
						for (StoreValue val : values) {
							response = response + val.getValue() + " ";
						}
					}
					
					req.response().putHeader("Content-Type", "text/plain");

					if (response != null)
						req.response().putHeader("Content-Length",
								String.valueOf(response.length()));
					req.response().end(response);
					req.response().close();
				}});
				t.start();
			}
		});

		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();

				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				Thread t = new Thread(new Runnable() {
				public void run() {
					// add the key to timequeue (wait for all data centers)
					add_to_queue(key, timestamp.toString(), "");

					req.response().putHeader("Content-Type", "text/plain");
					req.response().end();
					req.response().close();
				}});
				t.start();
			}
		});

		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();

				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				Thread t = new Thread(new Runnable() {
				public void run() {
					// remove the key to timequeue (all data centers ready)
					remove_from_queue(key, timestamp.toString());

					req.response().putHeader("Content-Type", "text/plain");
					req.response().end();
					req.response().close();
				}});
				t.start();
			}
		});

		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				keyValueStore.store.clear();

				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";

				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}


