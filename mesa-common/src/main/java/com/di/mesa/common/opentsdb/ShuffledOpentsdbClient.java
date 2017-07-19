package com.di.mesa.common.opentsdb;

import com.di.mesa.common.opentsdb.builder.Metric;
import com.di.mesa.common.opentsdb.builder.MetricBuilder;
import com.di.mesa.common.opentsdb.http.ExecutorApiClient;
import com.di.mesa.common.util.SplitUtil;
import jregex.Matcher;
import jregex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ShuffledOpentsdbClient {

    private static final Logger logger = LoggerFactory.getLogger(ShuffledOpentsdbClient.class);

    private String urls;
    private String[] splitUrls;

    private OpentsdbClient clientMaster;
    private OpentsdbClient clientSlave;

    private OpentsdbClient currentClient;
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
        this.splitUrls = SplitUtil.splitPreserveAllTokens(this.urls, ",");
        this.masterUrl = splitUrls[random.nextInt(splitUrls.length)];
        initSlaveUrl();

        this.clientMaster = new OpentsdbClient(this.masterUrl);
        this.clientSlave = new OpentsdbClient(this.slaveUrl);

        this.currentClient = this.clientMaster;
        this.clientState = 0;

        this.apiclient = ExecutorApiClient.getInstance();
        String patternString = ".*\"timestamp\":.*\"repo_status\":.*";
        this.pattern = new Pattern(patternString);
    }

    private void initSlaveUrl() {
        if (splitUrls.length > 1) {
            while (true) {
                this.slaveUrl = splitUrls[random.nextInt(splitUrls.length)];
                if (this.masterUrl != this.slaveUrl) {
                    break;
                }
            }
        } else {
            this.slaveUrl = splitUrls[random.nextInt(splitUrls.length)];
        }
    }

    private void initMasterUrl() {
        if (splitUrls.length > 1) {
            while (true) {
                this.masterUrl = splitUrls[random.nextInt(splitUrls.length)];
                if (this.masterUrl != this.slaveUrl) {
                    break;
                }
            }
        } else {
            this.masterUrl = splitUrls[random.nextInt(splitUrls.length)];
        }
    }

    public void resetMaster() {
        initMasterUrl();
        this.clientMaster = new OpentsdbClient(this.masterUrl);
    }

    private String getMasterUrl() {
        return this.masterUrl + (this.masterUrl.endsWith("/") ? "api/version" : "/api/version");
    }

    private String getSlaveUrl() {
        return this.slaveUrl + (this.slaveUrl.endsWith("/") ? "api/version" : "/api/version");
    }

    public void resetSlave() {
        initSlaveUrl();
        this.clientSlave = new OpentsdbClient(this.slaveUrl);
    }

    public void shuffleClient(boolean shouldActiveShuffle) {
        this.manuallyShuffled.set(shouldActiveShuffle);

        if (this.manuallyShuffled.get()) {
            currentLock.lock();
            if (clientState == 0) {
                this.currentClient = this.clientSlave;
                this.clientState = 1;
                resetMaster();
            } else {
                this.currentClient = this.clientMaster;
                this.clientState = 0;
                resetSlave();
            }
            currentLock.unlock();
        }
    }

    public void shuffleClientAutoly() {
        if (!this.manuallyShuffled.get()) {
            currentLock.lock();
            if (clientState == 0) {
                this.currentClient = this.clientSlave;
                this.clientState = 1;
                resetMaster();
            } else {
                this.currentClient = this.clientMaster;
                this.clientState = 0;
                resetSlave();
            }
            currentLock.unlock();
        } else {
            this.manuallyShuffled.set(false);
        }
    }

    public boolean putData(MetricBuilder builder) {
        currentLock.lock();
        boolean retVal = false;
        try {
            return currentClient.putData(builder);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.info("builder content : ");
            for (Metric metric : builder.getMetrics()) {
                logger.info(metric.toString());
            }
        } finally {
            currentLock.unlock();
        }
        return retVal;
    }

    public boolean checkService() throws IOException {
        URI uri = URI.create(this.currentClient.getOpentsdbUrl()
                + (this.currentClient.getOpentsdbUrl().endsWith("/") ? "api/version" : "/api/version"));
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

    public OpentsdbClient getCurrentClient() {
        return this.currentClient;
    }

    public void shutdown() {

        if (clientMaster != null) {
            clientMaster.shutdown();
        }

        if (clientSlave != null) {
            clientSlave.shutdown();
        }

    }

    public boolean getActiveShuffled() {
        return manuallyShuffled.get();
    }

    public void setActiveShuffled(boolean activeShuffled) {
        this.manuallyShuffled.set(activeShuffled);
    }

}
