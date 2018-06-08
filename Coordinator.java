import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.TreeMap;
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

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: causal, eventual, strong
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-54-172-59-62.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-52-23-179-206.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-54-172-227-145.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-172-181-21.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-54-164-167-252.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-54-84-105-117.compute-1.amazonaws.com";


	// A hashtable that maps the key to a sorted hashtable of timestamps 
	HashMap<String, TreeMap<String, String>> timeQueues = new HashMap<String, TreeMap<String, String>>();
	// a lock object for synchronization	
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

	// blocks until the key is ready to be processed
	private void wait_until_free(String key, String time, String value) {
		while (true) {
			synchronized (lock) {
				// make sures that this key is the first in order (time)
				if (timeQueues.get(key).firstKey().equals(time)){
					return;
				}
				// wait for another thread to wake up 
				try {lock.wait();} catch (Exception e) {}
			}
		}
	}

	// signals other waiting threads that a key just finished processing
	private void signal_done(String key, String time) {
		synchronized (lock) {
			// remove the key from both hashtables
			timeQueues.get(key).remove(time);
			// wake up waiting threads
			try {lock.notifyAll();} catch (Exception e) {}
		}
	}
	
	// return the local data center
	private String local_dc() {
		if (region == 1) return dataCenterUSE;
		else if (region == 2) return dataCenterUSW;
		else return dataCenterSING;
	}

	// return the local coordinator
	private String local_cr() {
		if (region == 1) return coordinatorUSE;
		else if (region == 2) return coordinatorUSW;
		else return coordinatorSING;
	}

	// hash function that converts a key to the coordinator
	private String key_to_cr(String key) {
		if (key.equals("a")) return coordinatorUSE;
		if (key.equals("b")) return coordinatorUSW;
		if (key.equals("c")) return coordinatorSING;

		int len = key.length();
		int i = 0;
		int sum = 0;
		char[] chars = key.toCharArray();

		// add the ascii value of each character in the string
		while (i < len) {
			sum += ((int)(chars[i]));
			i++;
		}

		// mod the sum by 3 to make sure only 3 possible results
		int j = (sum % 3) + 1;
		if (j == 1) return coordinatorUSE;
		if (j == 2) return coordinatorUSW;
		return coordinatorSING;
	}

	// fix the timestamp in case the put request was forwarded
	private String get_time(Long timestamp, String forwarded, String forwardedRegion) {
		if (forwarded == null) {
			return timestamp.toString();
		}
		else {
			if (forwardedRegion.equals("1")) {
				return (new Long(Skews.handleSkew(timestamp.longValue(), 1))).toString();
			}
			else if (forwardedRegion.equals("2")) {
				return (new Long(Skews.handleSkew(timestamp.longValue(), 2))).toString();
			}
			else {
				return (new Long(Skews.handleSkew(timestamp.longValue(), 3))).toString();
			}
		}
	}

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);

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
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				final String adjusted = get_time(timestamp, forwarded, forwardedRegion);

				Thread t = new Thread(new Runnable() {
					public void run() {
						try {
							String dest_cr = key_to_cr(key);

							// only process key as a primary coordinator
							if (dest_cr.equals(local_cr())) {
								// notify all data centers about the put operation
								if (consistencyType.equals("strong"))
									KeyValueLib.AHEAD(key, adjusted);

								// add key to time queue
								if (consistencyType.equals("strong"))
									add_to_queue(key, adjusted, value);

								// wait for the key to process in sorted timestamp order
								if (consistencyType.equals("strong"))
									wait_until_free(key, adjusted, value);

								// handle put operations to all data centers
								KeyValueLib.PUT(dataCenterUSE, key, value, adjusted, consistencyType);
								KeyValueLib.PUT(dataCenterUSW, key, value, adjusted, consistencyType);
								KeyValueLib.PUT(dataCenterSING, key, value, adjusted, consistencyType);

								// notify all data centers about the completion of this put
								if (consistencyType.equals("strong"))
									KeyValueLib.COMPLETE(key, adjusted);

								// wait up other waiting threads
								if (consistencyType.equals("strong"))
									signal_done(key, adjusted);	
							}
							else {
								// notify all data centers about the put operation
								if (consistencyType.equals("strong"))
									KeyValueLib.AHEAD(key, adjusted);

								// forward key to primary coordinator
								KeyValueLib.FORWARD(dest_cr, key, value, timestamp.toString());
							}
						} catch (Exception e) {}
					}
				});
				t.start();

				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();

				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				Thread t = new Thread(new Runnable() {
					public void run() {
						String response = "0";
						try {
							// do not block get requests, let data center handle the ordering
							response = KeyValueLib.GET(local_dc(), key, timestamp.toString(), consistencyType);
						} catch (Exception e) {}
						req.response().end(response);
					}
				});
				t.start();
			}
		});

		/* This endpoint is used by the grader to change the consistency level */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");

				req.response().end();
			}
		});

		/* BONUS HANDLERS BELOW */
		routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().end(KeyValueLib.COUNT());
			}
		});

		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				KeyValueLib.RESET();

				req.response().end();
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


