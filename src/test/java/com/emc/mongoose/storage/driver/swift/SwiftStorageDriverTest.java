package com.emc.mongoose.storage.driver.swift;

import com.emc.mongoose.api.common.env.DateUtil;
import com.emc.mongoose.api.model.data.DataInput;
import com.emc.mongoose.api.model.io.IoType;
import com.emc.mongoose.api.model.io.task.composite.data.BasicCompositeDataIoTask;
import com.emc.mongoose.api.model.io.task.composite.data.CompositeDataIoTask;
import com.emc.mongoose.api.model.io.task.data.BasicDataIoTask;
import com.emc.mongoose.api.model.io.task.data.DataIoTask;
import com.emc.mongoose.api.model.io.task.partial.data.PartialDataIoTask;
import com.emc.mongoose.api.model.item.BasicDataItem;
import com.emc.mongoose.api.model.item.DataItem;
import com.emc.mongoose.api.model.item.Item;
import com.emc.mongoose.api.model.item.ItemFactory;
import com.emc.mongoose.api.model.item.ItemType;
import com.emc.mongoose.api.model.storage.Credential;
import com.emc.mongoose.ui.config.Config;
import com.emc.mongoose.ui.config.load.LoadConfig;
import com.emc.mongoose.ui.config.load.batch.BatchConfig;
import com.emc.mongoose.ui.config.load.rate.LimitConfig;
import com.emc.mongoose.ui.config.storage.StorageConfig;
import com.emc.mongoose.ui.config.storage.auth.AuthConfig;
import com.emc.mongoose.ui.config.storage.driver.DriverConfig;
import com.emc.mongoose.ui.config.storage.driver.queue.QueueConfig;
import com.emc.mongoose.ui.config.storage.net.NetConfig;
import com.emc.mongoose.ui.config.storage.net.http.HttpConfig;

import com.emc.mongoose.ui.config.storage.net.node.NodeConfig;
import com.github.akurilov.commons.system.SizeInBytes;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Queue;

public class SwiftStorageDriverTest
extends SwiftStorageDriver {

	private static final Credential CREDENTIAL = Credential.getInstance(
		"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb"
	);
	private static final String AUTH_TOKEN = "AUTH_tk65840af9f6f74d1aaefac978cb8f0899";
	private static final String NS = "ns1";

	private static Config getConfig() {
		try {
			final Config config = new Config();
			final LoadConfig loadConfig = new LoadConfig();
			config.setLoadConfig(loadConfig);
			final BatchConfig batchConfig = new BatchConfig();
			loadConfig.setBatchConfig(batchConfig);
			batchConfig.setSize(4096);
			final LimitConfig limitConfig = new LimitConfig();
			loadConfig.setLimitConfig(limitConfig);
			limitConfig.setConcurrency(0);
			final StorageConfig storageConfig = new StorageConfig();
			config.setStorageConfig(storageConfig);
			final NetConfig netConfig = new NetConfig();
			storageConfig.setNetConfig(netConfig);
			netConfig.setTransport("epoll");
			netConfig.setReuseAddr(true);
			netConfig.setBindBacklogSize(0);
			netConfig.setKeepAlive(true);
			netConfig.setRcvBuf(new SizeInBytes(0));
			netConfig.setSndBuf(new SizeInBytes(0));
			netConfig.setSsl(false);
			netConfig.setTcpNoDelay(false);
			netConfig.setInterestOpQueued(false);
			netConfig.setLinger(0);
			netConfig.setTimeoutMilliSec(0);
			netConfig.setIoRatio(50);
			final NodeConfig nodeConfig = new NodeConfig();
			netConfig.setNodeConfig(nodeConfig);
			nodeConfig.setAddrs(Collections.singletonList("127.0.0.1"));
			nodeConfig.setPort(9024);
			nodeConfig.setConnAttemptsLimit(0);
			final HttpConfig httpConfig = new HttpConfig();
			netConfig.setHttpConfig(httpConfig);
			httpConfig.setNamespace(NS);
			httpConfig.setFsAccess(true);
			httpConfig.setVersioning(true);
			httpConfig.setHeaders(Collections.EMPTY_MAP);
			final AuthConfig authConfig = new AuthConfig();
			storageConfig.setAuthConfig(authConfig);
			authConfig.setUid(CREDENTIAL.getUid());
			authConfig.setToken(AUTH_TOKEN);
			authConfig.setSecret(CREDENTIAL.getSecret());
			final DriverConfig driverConfig = new DriverConfig();
			storageConfig.setDriverConfig(driverConfig);
			final QueueConfig queueConfig = new QueueConfig();
			driverConfig.setQueueConfig(queueConfig);
			queueConfig.setInput(1000000);
			queueConfig.setOutput(1000000);
			return config;
		} catch(final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	private final Queue<FullHttpRequest> httpRequestsLog = new ArrayDeque<>();

	public SwiftStorageDriverTest()
	throws Exception {
		this(getConfig());
	}

	private SwiftStorageDriverTest(final Config config)
	throws Exception {
		super(
			"test-storage-driver-swift",
			DataInput.getInstance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16),
			config.getLoadConfig(), config.getStorageConfig(),
			false
		);
	}

	@Override
	protected FullHttpResponse executeHttpRequest(final FullHttpRequest httpRequest) {
		httpRequestsLog.add(httpRequest);
		return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
	}

	@After
	public void tearDown() {
		httpRequestsLog.clear();
	}

	@Test
	public void testRequestNewAuthToken()
	throws Exception {

		requestNewAuthToken(credential);
		assertEquals(1, httpRequestsLog.size());

		final FullHttpRequest req = httpRequestsLog.poll();
		assertEquals(HttpMethod.GET, req.method());
		assertEquals(SwiftApi.AUTH_URI, req.uri());
		final HttpHeaders reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(CREDENTIAL.getUid(), reqHeaders.get(SwiftApi.KEY_X_AUTH_USER));
		assertEquals(CREDENTIAL.getSecret(), reqHeaders.get(SwiftApi.KEY_X_AUTH_KEY));
	}

	@Test
	public void testRequestNewPath()
	throws Exception {

		final String container = "/container0";
		assertEquals(container, requestNewPath(container));
		assertEquals(2, httpRequestsLog.size());

		FullHttpRequest req;
		HttpHeaders reqHeaders;
		Date reqDate;

		req = httpRequestsLog.poll();
		assertEquals(HttpMethod.HEAD, req.method());
		assertEquals(SwiftApi.URI_BASE + '/' + NS + container, req.uri());
		reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));

		req = httpRequestsLog.poll();
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals(SwiftApi.URI_BASE + '/' + NS + container, req.uri());
		reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(
			SwiftApi.DEFAULT_VERSIONS_LOCATION, reqHeaders.get(SwiftApi.KEY_X_VERSIONS_LOCATION)
		);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));
	}

	@Test
	public void testContainerListingTest()
	throws Exception {

		final ItemFactory itemFactory = ItemType.getItemFactory(ItemType.DATA);
		final String container = "/container1";
		final String itemPrefix = "0000";
		final String markerItemId = "00003brre8lgz";
		final Item markerItem = itemFactory.getItem(
			markerItemId, Long.parseLong(markerItemId, Character.MAX_RADIX), 10240
		);

		final List<Item> items = list(
			itemFactory, container, itemPrefix, Character.MAX_RADIX, markerItem, 1000
		);

		assertEquals(0, items.size());
		assertEquals(1, httpRequestsLog.size());
		final FullHttpRequest req = httpRequestsLog.poll();
		assertEquals(HttpMethod.GET, req.method());
		assertEquals(
			SwiftApi.URI_BASE + '/' + NS + container + "?format=json&prefix=" + itemPrefix
				+ "&marker=" + markerItemId + "&limit=1000",
			req.uri()
		);
		final HttpHeaders reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));
	}

	@Test
	public void testCopyRequest()
	throws Exception {

		final String containerSrcName = "/containerSrc";
		final String containerDstName = "/containerDst";
		final long itemSize = 10240;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new BasicDataItem(
			itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize
		);
		final DataIoTask<DataItem> ioTask = new BasicDataIoTask<>(
			hashCode(), IoType.CREATE, dataItem, containerSrcName, containerDstName, CREDENTIAL,
			null, 0
		);
		final HttpRequest req = getHttpRequest(ioTask, storageNodeAddrs[0]);

		assertEquals(HttpMethod.PUT, req.method());
		assertEquals(SwiftApi.URI_BASE + '/' + NS + containerDstName + '/' + itemId, req.uri());
		final HttpHeaders reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(
			SwiftApi.URI_BASE + '/' + NS + containerSrcName + '/' + itemId,
			reqHeaders.get(SwiftApi.KEY_X_COPY_FROM)
		);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));
	}

	@Test
	public void testCreateDloPartRequest()
	throws Exception {
		final String container = "/container2";
		final long itemSize = 12345;
		final long partSize = 1234;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new BasicDataItem(
			itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize
		);
		final CompositeDataIoTask<DataItem> dloTask = new BasicCompositeDataIoTask<>(
			hashCode(), IoType.CREATE, dataItem, null, container, CREDENTIAL, null, 0, partSize
		);
		final PartialDataIoTask<DataItem> dloSubTask = dloTask.getSubTasks().get(0);
		final HttpRequest req = getHttpRequest(dloSubTask, storageNodeAddrs[0]);
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals(
			SwiftApi.URI_BASE + '/' + NS + container + '/' + itemId + "/0000001",  req.uri()
		);
		final HttpHeaders reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(partSize, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));
	}

	@Test
	public void testCreateDloManifestRequest()
	throws Exception {

		final String container = "container2";
		final long itemSize = 12345;
		final long partSize = 1234;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new BasicDataItem(
			itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize
		);
		final CompositeDataIoTask<DataItem> dloTask = new BasicCompositeDataIoTask<>(
			hashCode(), IoType.CREATE, dataItem, null, '/' + container, CREDENTIAL, null, 0,
			partSize
		);

		// emulate DLO parts creation
		final List<? extends PartialDataIoTask<DataItem>> subTasks = dloTask.getSubTasks();
		for(final PartialDataIoTask<DataItem> subTask : subTasks) {
			subTask.startRequest();
			subTask.finishRequest();
			subTask.startResponse();
			subTask.finishResponse();
		}
		assertTrue(dloTask.allSubTasksDone());

		final HttpRequest req = getHttpRequest(dloTask, storageNodeAddrs[0]);
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals(SwiftApi.URI_BASE + '/' + NS + '/' + container + '/' + itemId,  req.uri());
		final HttpHeaders reqHeaders = req.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		assertEquals(container + '/' + itemId + '/', reqHeaders.get(SwiftApi.KEY_X_OBJECT_MANIFEST));
		final Date reqDate = DateUtil.FMT_DATE_RFC1123.parse(reqHeaders.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate.getTime(), 10_000);
		assertEquals(AUTH_TOKEN, reqHeaders.get(SwiftApi.KEY_X_AUTH_TOKEN));
	}
}
