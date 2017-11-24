package com.di.mesa.metric.client;

import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.di.mesa.metric.client.util.ExecutorApiClient;
import com.di.mesa.metric.common.CommonConstant;
import com.di.mesa.metric.util.SplitUtil;
import jregex.Matcher;
import jregex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffledOpentsdbClient {

	private static final Logger logger = LoggerFactory.getLogger(ShuffledOpentsdbClient.class);

	private String urls;
	private String[] splitUrls;

	private String currentUrl;
	private String masterUrl;
	private String slaveUrl;

	private int clientState;

	private AtomicBoolean manuallyShuffled = new AtomicBoolean(false);

	private ReentrantLock currentLock = new ReentrantLock();
	private Random random = new Random();

	private ExecutorApiClient apiclient = null;
	private Pattern pattern;

	public ShuffledOpentsdbClient(String urls) {
		this.urls = urls;
		this.splitUrls = SplitUtil.splitPreserveAllTokens(this.urls, CommonConstant._COMMA);
		this.masterUrl = splitUrls[random.nextInt(splitUrls.length)];
		initSlaveUrl();

		this.currentUrl = this.masterUrl;
		this.clientState = 0;

		this.apiclient = ExecutorApiClient.getInstance();
		this.pattern = new Pattern(".*\"timestamp\":.*\"repo_status\":.*");
		logger.info("opentsdb dao initiallized ,master url {}. slave url {}", this.masterUrl, this.slaveUrl);
	}

	private void initSlaveUrl() {
		while (true) {
			this.slaveUrl = splitUrls[random.nextInt(splitUrls.length)];
			if (this.splitUrls.length == 1 || this.masterUrl != this.slaveUrl) {
				break;
			}
		}
	}

	private void initMasterUrl() {
		while (true) {
			this.masterUrl = splitUrls[random.nextInt(splitUrls.length)];
			if (this.splitUrls.length == 1 || this.masterUrl != this.slaveUrl) {
				break;
			}
		}
	}

	public void resetMasterUrl() {
		initMasterUrl();
	}

	public void resetSlaveUrl() {
		initSlaveUrl();
	}

	public void shuffleClient(boolean shouldActiveShuffle) {
		this.manuallyShuffled.set(shouldActiveShuffle);

		if (this.manuallyShuffled.get()) {
			currentLock.lock();
			if (clientState == 0) {
				this.currentUrl = this.slaveUrl;
				this.clientState = 1;
				resetMasterUrl();
			} else {
				this.currentUrl = this.masterUrl;
				this.clientState = 0;
				resetSlaveUrl();
			}
			currentLock.unlock();
		}
	}

	public void shuffleClientAutoly() {
		if (!this.manuallyShuffled.get()) {
			currentLock.lock();
			if (clientState == 0) {
				this.currentUrl = this.slaveUrl;
				this.clientState = 1;
				resetMasterUrl();
			} else {
				this.currentUrl = this.masterUrl;
				this.clientState = 0;
				resetSlaveUrl();
			}
			currentLock.unlock();
		} else {
			this.manuallyShuffled.set(false);
		}
	}

	public boolean checkService() throws IOException {
		String baseUrl = this.currentUrl.substring(0, this.currentUrl.indexOf("/", 7));
		URI uri = URI.create(baseUrl + (baseUrl.endsWith("/") ? "api/version" : "/api/version"));
		String response = apiclient.httpGet(uri, null);
		return serviceAvailable(response);
	}

	private boolean serviceAvailable(String text) {
		Matcher matcher = pattern.matcher(text);
		return matcher.matches();
	}

	public int getClientState() {
		return clientState;
	}

	public void setClientState(int clientState) {
		this.clientState = clientState;
	}

	public String getCurrentUrl() {
		return this.currentUrl;
	}

	public void shutdown() {

	}

	public boolean getActiveShuffled() {
		return manuallyShuffled.get();
	}

	public void setActiveShuffled(boolean activeShuffled) {
		this.manuallyShuffled.set(activeShuffled);
	}

}
