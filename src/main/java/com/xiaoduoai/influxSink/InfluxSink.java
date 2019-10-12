package com.xiaoduoai.influxSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public class InfluxSink extends AbstractSink implements Configurable {
	private static final Logger LOG = Logger.getLogger(InfluxSink.class);
	private String url;
	private int batchSize;
	private String database;
	private String username;
	private String password;
	private String policy;
	private String influxsource;
	private InfluxDB influxDB;
	private SinkCounter sinkCounter;
	private String regex;
	private String measurement;
	private String colume;
	private String exclude;
	private String include;

	public void configure(Context context) {
		String host = context.getString("host", "localhost");
		String port = context.getString("port", "8086");
		String database = context.getString("database", "flumetest");
		int batchSize = context.getInteger("batchSize", 100);
		String username = context.getString("username", "root");
		String password = context.getString("password", "root");
		String influxsource = context.getString("influxsource", "body");
		String regex = context.getString("regex", "");
		String measurement = context.getString("measurement", "default");
		String colume = context.getString("colume", "");
		String policy = context.getString("policy", "default");
		String exclude = context.getString("exclude", "");
		String include = context.getString("include", "");
		String url = "http://" + host + ":" + port;
		this.url = url;
		this.batchSize = batchSize;
		this.database = database;
		this.username = username;
		this.password = password;
		this.influxsource = influxsource;
		this.regex = regex;
		this.policy = policy;
		this.measurement = measurement;
		this.colume = colume;
		this.exclude = exclude;
		this.include = include;

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	@Override
	public void start() {
		LOG.info("Starting Influx Sink {} ...Connecting to " + url);
		try {
			InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
			this.influxDB = influxDB;
			sinkCounter.incrementConnectionCreatedCount();
		}

		catch (Throwable e) {
			LOG.error(e.getStackTrace());
			sinkCounter.incrementConnectionFailedCount();
		}
		sinkCounter.start();
	}

	@Override
	public void stop() {
		LOG.info("Stopping Influx Sink {} ...");
		sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
	}

	public Status process() throws EventDeliveryException {
		Status status = null;
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
			ArrayList<Map<String, String>> headerlist = new ArrayList<Map<String, String>>();
			Event event = null;
			int count = 0;
			sinkCounter.incrementEventDrainAttemptCount();
			for (count = 0; count <= batchSize; ++count) {
				event = ch.take();
				if (event == null) {
					break;
				}
				Map<String, String> headers = event.getHeaders();
				Map<String, String> InfluxEvent = ExtractInfluxEvent(event, influxsource, regex, colume);
				if (InfluxEvent != null) {
					list.add(InfluxEvent);
					if (!headers.isEmpty()) {
						headerlist.add(headers);
					}
				}
				sinkCounter.incrementConnectionCreatedCount();

			}
			if (count <= 0) {
				sinkCounter.incrementBatchEmptyCount();
				sinkCounter.incrementEventDrainSuccessCount();
				status = Status.BACKOFF;
			} else {
				try {
					BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(policy)
							.consistency(InfluxDB.ConsistencyLevel.ONE).build();

					for (int i = 0; i < list.size(); i++) {
						Point.Builder builder = Point.measurement(measurement).time(System.currentTimeMillis(),
								TimeUnit.MILLISECONDS);
						Map<String, String> bodymap = list.get(i);
						if (!headerlist.isEmpty()) {
							Map<String, String> headermap = headerlist.get(i);
							if (!headermap.isEmpty()) {
								for (Map.Entry<String, String> entry : headermap.entrySet()) {
									builder.tag(entry.getKey(), entry.getValue());
								}

							}
						}

						if (!bodymap.isEmpty()) {
							for (Map.Entry<String, String> entry : bodymap.entrySet()) {
								if (!isInteger(entry.getValue().toString())) {
									if (entry.getValue().trim() == "") {
										builder.tag(entry.getKey(), "other");
									} else {
										builder.tag(entry.getKey(), entry.getValue().trim());
									}
								} else {
									builder.addField(entry.getKey(), Float.parseFloat(entry.getValue().trim()));
								}
							}
						} else {
							LOG.info("body map is null");
							continue;
						}
						Point point = builder.build();
						batchPoints.point(point);
					}
					influxDB.write(batchPoints);
					// influxDB.write(database, policy,
					// InfluxDB.ConsistencyLevel.ONE, batch.toString());
					status = Status.READY;
					if (count < batchSize) {
						sinkCounter.incrementBatchUnderflowCount();
					}
					sinkCounter.incrementBatchCompleteCount();
				} catch (Exception e) {
					e.printStackTrace();
					LOG.info(e.getMessage());
					// txn.rollback();
					status = Status.BACKOFF;
					sinkCounter.incrementConnectionFailedCount();
				}
			}
			txn.commit();
			if (event == null) {
				status = Status.BACKOFF;
			}

			return status;
		} catch (Throwable t) {
			txn.rollback();
			// Log exception, handle individual exceptions as needed
			LOG.info(t.getMessage());
			status = Status.BACKOFF;

			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

	private Map<String, String> ExtractInfluxEvent(Event event, String influx_source, String regex, String colume) {
		Map<String, String> map = new HashMap<String, String>();
		if (influx_source.equals("body")) {
			String body = new String(event.getBody());
			// Fix for data coming from windows
			body = body.replaceAll("\\r", "");
			Pattern pattern = Pattern.compile(this.regex);
			String[] columearray = colume.split(",");
			if (this.exclude != "") {
				String[] excludearray = this.exclude.split(",");
				for (int i = 0; i < excludearray.length; i++) {
					if (body.indexOf(excludearray[i]) != -1) {
						return null;
					}
				}
			}
			if (this.include != "") {
				String[] includearray = this.include.split(",");
				for (int i = 0; i < includearray.length; i++) {
					if (body.indexOf(includearray[i]) == -1) {
						return null;
					}
				}
			}
			Matcher matcher = pattern.matcher(body);
			if (matcher.find()) {
				int grouplength = matcher.groupCount();
				int columelength = columearray.length;
				if (grouplength != columelength) {
					LOG.info("match fail count error !" + "the columelength count is" + columelength
							+ " the group count is" + grouplength + " the log is:" + body);
					for (int i = 0; i < grouplength; i++) {
						System.out.println("group is:" + matcher.group(i + 1));
					}
					return null;
				}
				for (int i = 0; i < grouplength; i++) {
					map.put(columearray[i], matcher.group(i + 1));
				}
			} else {
				LOG.info("match fail !" + "the match find is" + matcher.find() + " the match rule is" + this.regex
						+ " the log is:" + body);
			}
			return map;

		} else {
			LOG.error("Just body is supported for the moment");
			return null;
		}

	}

	private boolean isInteger(String str) {
		str = str.trim();
		Pattern pattern = Pattern.compile("-?[0-9]+\\.?[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

}