package net.anumbrella.seaweedfs.core;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.anumbrella.seaweedfs.core.content.ForceGarbageCollectionParams;
import net.anumbrella.seaweedfs.core.content.LookupVolumeResult;
import net.anumbrella.seaweedfs.core.content.PreAllocateVolumesParams;
import net.anumbrella.seaweedfs.core.http.HeaderResponse;
import net.anumbrella.seaweedfs.core.http.JsonResponse;
import net.anumbrella.seaweedfs.core.http.StreamResponse;
import net.anumbrella.seaweedfs.core.topology.*;
import net.anumbrella.seaweedfs.exception.SeaweedfsException;
import net.anumbrella.seaweedfs.util.ConnectionUtil;
import net.anumbrella.seaweedfs.util.RequestPathStrategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.http.HttpEntity;
//import org.apache.http.client.cache.HttpCacheStorage;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpHead;
//import org.apache.http.client.methods.HttpRequestBase;
//import org.apache.http.client.protocol.HttpClientContext;
//import org.apache.http.config.SocketConfig;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.http.impl.client.cache.CacheConfig;
//import org.apache.http.impl.client.cache.CachingHttpClients;
//import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
//import org.apache.http.util.EntityUtils;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.http2.Header;

public class Connection {

    static final String LOOKUP_VOLUME_CACHE_ALIAS = "lookupVolumeCache";

    private static final Log log = LogFactory.getLog(Connection.class);

    private String leaderUrl;
    private long statusExpiry;
    private int connectionTimeout;
    private boolean connectionClose = true;
    private boolean enableFileStreamCache;
    private int fileStreamCacheEntries;
    private long fileStreamCacheSize;
    //    private HttpCacheStorage fileStreamCacheStorage;
    private boolean enableLookupVolumeCache;
    private long lookupVolumeCacheExpiry;
    private int lookupVolumeCacheEntries;
    private long idleConnectionExpiry;
    private SystemClusterStatus systemClusterStatus;
    private SystemTopologyStatus systemTopologyStatus;
    private PollClusterStatusThread pollClusterStatusThread;
    private ObjectMapper objectMapper = new ObjectMapper();
    //    private PoolingHttpClientConnectionManager clientConnectionManager;
    private IdleConnectionMonitorThread idleConnectionMonitorThread;
    private OkHttpClient httpClient;
    private CacheManager cacheManager = null;

//    public Connection(String leaderUrl, int connectionTimeout, long statusExpiry, long idleConnectionExpiry,
//                      int maxConnection, int maxConnectionsPreRoute, boolean enableLookupVolumeCache,
//                      long lookupVolumeCacheExpiry, int lookupVolumeCacheEntries, boolean enableFileStreamCache,
//                      int fileStreamCacheEntries, long fileStreamCacheSize, HttpCacheStorage fileStreamCacheStorage) {
//        this.leaderUrl = leaderUrl;
//        this.statusExpiry = statusExpiry;
//        this.connectionTimeout = connectionTimeout;
//        this.idleConnectionExpiry = idleConnectionExpiry;
//        this.enableLookupVolumeCache = enableLookupVolumeCache;
//        this.lookupVolumeCacheExpiry = lookupVolumeCacheExpiry;
//        this.lookupVolumeCacheEntries = lookupVolumeCacheEntries;
//        this.pollClusterStatusThread = new PollClusterStatusThread();
//        this.idleConnectionMonitorThread = new IdleConnectionMonitorThread();
////        this.clientConnectionManager = new PoolingHttpClientConnectionManager();
////        this.clientConnectionManager.setMaxTotal(maxConnection);
////        this.clientConnectionManager.setDefaultMaxPerRoute(maxConnectionsPreRoute);
//        this.enableFileStreamCache = enableFileStreamCache;
//        this.fileStreamCacheEntries = fileStreamCacheEntries;
//        this.fileStreamCacheSize = fileStreamCacheSize;
////        this.fileStreamCacheStorage = fileStreamCacheStorage;
//    }



    public Connection(String leaderUrl, int connectionTimeout, long statusExpiry, long idleConnectionExpiry,
                      int maxConnection, int maxConnectionsPreRoute, boolean enableLookupVolumeCache,
                      long lookupVolumeCacheExpiry, int lookupVolumeCacheEntries, boolean enableFileStreamCache,
                      int fileStreamCacheEntries, long fileStreamCacheSize) {
        this.leaderUrl = leaderUrl;
        this.statusExpiry = statusExpiry;
        this.connectionTimeout = connectionTimeout;
        this.idleConnectionExpiry = idleConnectionExpiry;
        this.enableLookupVolumeCache = enableLookupVolumeCache;
        this.lookupVolumeCacheExpiry = lookupVolumeCacheExpiry;
        this.lookupVolumeCacheEntries = lookupVolumeCacheEntries;
        this.pollClusterStatusThread = new PollClusterStatusThread();
        this.idleConnectionMonitorThread = new IdleConnectionMonitorThread();
//        this.clientConnectionManager = new PoolingHttpClientConnectionManager();
//        this.clientConnectionManager.setMaxTotal(maxConnection);
//        this.clientConnectionManager.setDefaultMaxPerRoute(maxConnectionsPreRoute);
        this.enableFileStreamCache = enableFileStreamCache;
        this.fileStreamCacheEntries = fileStreamCacheEntries;
        this.fileStreamCacheSize = fileStreamCacheSize;
//        this.fileStreamCacheStorage = fileStreamCacheStorage;
    }

    /**
     * Start up polls for core leader.
     */
    public void startup() {
//        final RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(this.connectionTimeout)
//                .setSocketTimeout(connectionTimeout)
//                // 一个connection可以有多个request
//                .setConnectionRequestTimeout(connectionTimeout).build();
//        // Create socket configuration
//        SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).setSoKeepAlive(true)
//                .setSoTimeout(connectionTimeout).build();
//        clientConnectionManager.setDefaultSocketConfig(socketConfig);
//        if (this.enableFileStreamCache) {
//            if (this.fileStreamCacheStorage == null) {
//                final CacheConfig cacheConfig = CacheConfig.custom().setMaxCacheEntries(this.fileStreamCacheEntries)
//                        .setMaxObjectSize(this.fileStreamCacheSize).setHeuristicCachingEnabled(true)
//                        .setHeuristicCoefficient(0.8f).build();
//                this.httpClient = CachingHttpClients.custom().setCacheConfig(cacheConfig)
//                        .setConnectionManager(this.clientConnectionManager).setDefaultRequestConfig(requestConfig)
//                        .build();
//            } else {
//                this.httpClient = CachingHttpClients.custom().setHttpCacheStorage(this.fileStreamCacheStorage)
//                        .setConnectionManager(this.clientConnectionManager).setDefaultRequestConfig(requestConfig)
//                        .build();
//            }
//        } else {
//            this.httpClient = HttpClients.custom().setConnectionManager(this.clientConnectionManager)
//                    .setDefaultRequestConfig(requestConfig).build();
//        }

        this.httpClient = new OkHttpClient()
                .newBuilder()
                .build();

        initCache();
        this.pollClusterStatusThread.updateSystemStatus(true, true);
        this.pollClusterStatusThread.start();
        this.idleConnectionMonitorThread.start();
        log.info("seaweedfs master server connection is startup");
    }

    /**
     * Init cache manager and cache mapping.
     */
    private void initCache() {
        if (enableLookupVolumeCache) {
            CacheManagerBuilder builder = CacheManagerBuilder.newCacheManagerBuilder();
            this.cacheManager = builder.build(true);
            if(enableLookupVolumeCache){
                this.cacheManager.createCache(LOOKUP_VOLUME_CACHE_ALIAS,
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(Long.class, LookupVolumeResult.class,
                                        ResourcePoolsBuilder.heap(this.lookupVolumeCacheEntries))
                                .withExpiry(ExpiryPolicyBuilder
                                        .timeToLiveExpiration(Duration.ofSeconds(this.lookupVolumeCacheExpiry)))
                                .build());
            }
        }
    }

//    private Duration create(long seconds) {
//        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
//            return Duration.ofSeconds(seconds);
//        } else {
//            if ((seconds) == 0) {
//                return Duration.ZERO;
//            }
//            return new Duration(seconds, 0);
//        }
//    }

    /**
     * Shutdown polls for core leader.
     */
    public void stop() {
        closeCache();
        this.pollClusterStatusThread.shutdown();
        this.idleConnectionMonitorThread.shutdown();
        log.info("seaweedfs master server connection is shutdown");
    }

    /**
     * Get core server cluster status.
     *
     * @return Core cluster status.
     */
    public SystemClusterStatus getSystemClusterStatus() {
        return systemClusterStatus;
    }

    /**
     * Get cluster topology status.
     *
     * @return Core topology status.
     */
    public SystemTopologyStatus getSystemTopologyStatus() {
        return systemTopologyStatus;
    }

    /**
     * Close all cache and close cache manager.
     */
    private void closeCache() {
        if (cacheManager != null) {
            cacheManager.removeCache(LOOKUP_VOLUME_CACHE_ALIAS);
            cacheManager.close();
        }
    }

    /**
     * Connection close flag.
     *
     * @return If result is false, that maybe core server is failover.
     */
    public boolean isConnectionClose() {
        return connectionClose;
    }

    /**
     * Get cache manager.
     *
     * @return {@code null} if no such cache exists.
     */
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * Get leader core server uri.
     *
     * @return Core server uri
     */
    public String getLeaderUrl() {
        return this.leaderUrl;
    }

    /**
     * Thread for cycle to check cluster status.
     */
    private class PollClusterStatusThread extends Thread {

        private volatile boolean shutdown;

        @Override
        public void run() {
            while (!shutdown) {
                synchronized (this) {
                    updateSystemStatus(false, false);
                }
            }
        }

        void updateSystemStatus(boolean immediate, boolean disposable) {
            if (!immediate) {
                try {
                    Thread.sleep(statusExpiry * 1000);
                } catch (InterruptedException ignored) {
                }
            }

            try {
                fetchSystemStatus(leaderUrl);
                connectionClose = false;
            } catch (IOException e) {
                connectionClose = true;
            }

            try {
                if (connectionClose) {
                    log.info("lookup seaweedfs core leader by peers");
                    if (systemClusterStatus == null || systemClusterStatus.getPeers().size() == 0) {
                    } else {
                        String url = findLeaderUriByPeers(systemClusterStatus.getPeers());
                        if (url != null) {
                            fetchSystemStatus(url);
                            connectionClose = false;
                        } else {
                            log.error("seaweedfs core cluster is down");
                            systemClusterStatus.getLeader().setActive(false);
                            connectionClose = true;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (immediate && !disposable) {
                try {
                    Thread.sleep(statusExpiry * 1000);
                } catch (InterruptedException ignored) {
                }
            }
        }

        private void fetchSystemStatus(String url) throws IOException {
            systemClusterStatus = fetchSystemClusterStatus(url);
            systemTopologyStatus = fetchSystemTopologyStatus(url);
            if (!leaderUrl.equals(systemClusterStatus.getLeader().getUrl())) {
                leaderUrl = (systemClusterStatus.getLeader().getUrl());
            }
        }

        private void shutdown() {
            this.shutdown = true;
            this.interrupt();
            synchronized (this) {
                notifyAll();
            }
        }
    }

    /**
     * Find leader core server from peer core server info.
     *
     * @param peers Peers core server.
     * @return If not found the leader, result is null.
     */
    private String findLeaderUriByPeers(List<MasterStatus> peers) throws IOException {
        if (peers == null || peers.size() == 0)
            return null;
        else {
            String result;
            for (MasterStatus item : peers) {
                final Request request = new Request.Builder().url(item.getUrl() + RequestPathStrategy.checkClusterStatus).get().build();
//                final HttpGet request = new HttpGet(item.getUrl() + RequestPathStrategy.checkClusterStatus);
                Map responseMap;
                try {
                    final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
                    responseMap = objectMapper.readValue(jsonResponse.json, Map.class);
                } catch (IOException e) {
                    continue;
                }
                if (responseMap.get("Leader") != null) {
                    result = ConnectionUtil.convertUrlWithScheme((String) responseMap.get("Leader"));

                    if (ConnectionUtil.checkUriAlive(httpClient, result))
                        return result;
                }
            }
        }
        return null;
    }

    /**
     * Fetch core server by seaweedfs Http API.
     *
     * @param masterUrl Core server url with scheme.
     * @return Cluster status.
     */
    @SuppressWarnings("unchecked")
    private SystemClusterStatus fetchSystemClusterStatus(String masterUrl) throws IOException {
        MasterStatus leader;
        ArrayList<MasterStatus> peers;
//        final HttpGet request = new HttpGet(masterUrl + RequestPathStrategy.checkClusterStatus);
        final Request request = new Request.Builder().url(masterUrl + RequestPathStrategy.checkClusterStatus).get().build();
        final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
        Map map = objectMapper.readValue(jsonResponse.json, Map.class);

        // 不应该直接指定leader,应该先判断是否leader
        // 比如在docker环境中，masterUrl是leader,但是返回的Leader值是docker的容器ip
        if (map.get("IsLeader") != null && ((Boolean) map.get("IsLeader"))) {
            leader = new MasterStatus(masterUrl);
        } else {
            if (map.get("Leader") != null) {
                leader = new MasterStatus((String) map.get("Leader"));
            } else {
                throw new SeaweedfsException("not found seaweedfs core leader");
            }
        }

        peers = new ArrayList<>();

        if (map.get("Peers") != null) {
            List<String> rawPeerList = (List<String>) map.get("Peers");
            for (String url : rawPeerList) {
                MasterStatus peer = new MasterStatus(url);
                peers.add(peer);
            }
        }

        if (map.get("IsLeader") == null || !((Boolean) map.get("IsLeader"))) {
            peers.add(new MasterStatus(masterUrl.replace("http://", "")));
            peers.remove(leader);
            leader.setActive(ConnectionUtil.checkUriAlive(this.httpClient, leader.getUrl()));
            if (!leader.isActive())
                throw new SeaweedfsException("seaweedfs core leader is failover");
        } else {
            leader.setActive(true);
        }

        for (MasterStatus item : peers) {
            item.setActive(ConnectionUtil.checkUriAlive(this.httpClient, item.getUrl()));
        }

        return new SystemClusterStatus(leader, peers);

    }

    /**
     * Fetch topology by seaweedfs Http Api.
     *
     * @param masterUrl Core server url with scheme.
     * @return Topology status.
     */
    @SuppressWarnings("unchecked")
    private SystemTopologyStatus fetchSystemTopologyStatus(String masterUrl) throws IOException {
//        final HttpGet request = new HttpGet(masterUrl + RequestPathStrategy.checkTopologyStatus);
        final Request request = new Request.Builder().url(masterUrl + RequestPathStrategy.checkTopologyStatus).get().build();
        final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
        Map map = objectMapper.readValue(jsonResponse.json, Map.class);

        // Fetch data center from json
        List<DataCenter> dataCenters = new ArrayList<>();
        ArrayList<Map<String, Object>> rawDcs = ((ArrayList<Map<String, Object>>) ((Map) (map.get("Topology")))
                .get("DataCenters"));
        if (rawDcs != null)
            for (Map<String, Object> rawDc : rawDcs) {
                DataCenter dc = new DataCenter();
                dc.setFree((Integer) rawDc.get("Free"));
                dc.setId((String) rawDc.get("Id"));
                dc.setMax((Integer) rawDc.get("Max"));

                List<Rack> racks = new ArrayList<Rack>();
                ArrayList<Map<String, Object>> rawRks = ((ArrayList<Map<String, Object>>) (rawDc.get("Racks")));
                if (rawRks != null)
                    for (Map<String, Object> rawRk : rawRks) {
                        Rack rk = new Rack();
                        rk.setMax((Integer) rawRk.get("Max"));
                        rk.setId((String) rawRk.get("Id"));
                        rk.setFree((Integer) rawRk.get("Free"));

                        List<DataNode> dataNodes = new ArrayList<DataNode>();
                        ArrayList<Map<String, Object>> rawDns = ((ArrayList<Map<String, Object>>) (rawRk
                                .get("DataNodes")));

                        if (rawDns != null)
                            for (Map<String, Object> rawDn : rawDns) {
                                DataNode dn = new DataNode();
                                dn.setFree((Integer) rawDn.get("Free"));
                                dn.setMax((Integer) rawDn.get("Max"));
                                dn.setVolumes((Integer) rawDn.get("Volumes"));
                                dn.setUrl((String) rawDn.get("Url"));
                                dn.setPublicUrl((String) rawDn.get("PublicUrl"));
                                dataNodes.add(dn);
                            }
                        rk.setDataNodes(dataNodes);
                        racks.add(rk);
                    }
                dc.setRacks(racks);
                dataCenters.add(dc);
            }

        // Fetch data layout
        ArrayList<Layout> layouts = new ArrayList<>();
        ArrayList<Map<String, Object>> rawLos = ((ArrayList<Map<String, Object>>) ((Map) (map.get("Topology")))
                .get("layouts"));
        if (rawLos != null)
            for (Map<String, Object> rawLo : rawLos) {
                Layout layout = new Layout();
                if (rawLo.get("collection") != null || !((String) rawLo.get("collection")).isEmpty()) {
                    layout.setCollection((String) rawLo.get("collection"));
                }
                if (rawLo.get("replication") != null || !((String) rawLo.get("replication")).isEmpty()) {
                    layout.setReplication((String) rawLo.get("replication"));
                }
                if (rawLo.get("ttl") != null || !((String) rawLo.get("ttl")).isEmpty()) {
                    layout.setTtl((String) rawLo.get("ttl"));
                }
                if (rawLo.get("writables") != null) {
                    layout.setWritables(((ArrayList<Integer>) rawLo.get("writables")));
                }
                layouts.add(layout);
            }

        SystemTopologyStatus systemTopologyStatus = new SystemTopologyStatus();
        systemTopologyStatus.setDataCenters(dataCenters);
        systemTopologyStatus.setLayouts(layouts);
        systemTopologyStatus.setFree((Integer) ((Map) (map.get("Topology"))).get("Free"));
        systemTopologyStatus.setMax((Integer) ((Map) (map.get("Topology"))).get("Max"));
        systemTopologyStatus.setVersion((String) map.get("Version"));

        return systemTopologyStatus;
    }

    /**
     * Fetch http API json result.
     *
     * @param request Http request.
     * @return Json fetch by http response.
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public JsonResponse fetchJsonResultByRequest(Request request) throws IOException {
//        CloseableHttpResponse response = null;
        Response response = null;
        Call call = null;
        JsonResponse jsonResponse = null;

        try {
            call = this.httpClient.newCall(request);
            response = call.execute();

            ResponseBody body = response.body();
            if (body != null) {
                jsonResponse = new JsonResponse(body.string(), response.code());
            } else {
                jsonResponse = new JsonResponse("", response.code());
            }

//            response = httpClient.execute(request, HttpClientContext.create());
//            HttpEntity entity = response.getEntity();
//            if (entity != null) {
//                jsonResponse = new JsonResponse(EntityUtils.toString(entity), response.getStatusLine().getStatusCode());
//                EntityUtils.consume(entity);
//            } else {
//                //SeaweedFS在删除的时候，经常会只返回一个204，这里处理204代码
//                jsonResponse = new JsonResponse("", response.getStatusLine().getStatusCode());
//            }
        } catch (Exception e) {
            log.error("request url " + request.url(), e);
        } finally {
//            if (call != null) {
//                try {
//                    call.cancel();
//                } catch (Exception e) {
//                    log.error("close call url " + request.url(), e);
//                }
//            }
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    log.error("close request url " + request.url(), e);
                }
            }
//            request.releaseConnection();
            request = null;
        }

        if (jsonResponse != null && jsonResponse.json.contains("\"error\":\"")) {
            Map map = objectMapper.readValue(jsonResponse.json, Map.class);
            final String errorMsg = (String) map.get("error");
            if (errorMsg != null) {
                throw new SeaweedfsException(errorMsg);
            }
        }

        return jsonResponse;
    }

    /**
     * Fetch http API status code.
     *
     * @param request Only http method head.
     * @return Status code.
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public int fetchStatusCodeByRequest(Request request) throws IOException {
//        CloseableHttpResponse response = null;
        Call call = null;
        Response response = null;
        int statusCode;
        try {
            call = httpClient.newCall(request);
            response = call.execute();
            statusCode = response.code();
//            response = httpClient.execute(request, HttpClientContext.create());
//            statusCode = response.getStatusLine().getStatusCode();
        } finally {
//            if (call != null) {
//                call.cancel();
//            }
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            request = null;
//            request.releaseConnection();
        }
        return statusCode;
    }

    /**
     * Force garbage collection.
     *
     * @param garbageThreshold Garbage threshold.
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public void forceGarbageCollection(float garbageThreshold) throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.forceGarbageCollection(new ForceGarbageCollectionParams(garbageThreshold));
    }

    /**
     * Force garbage collection.
     *
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public void forceGarbageCollection() throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.forceGarbageCollection(new ForceGarbageCollectionParams());
    }

    /**
     * Pre-allocate volumes.
     *
     * @param sameRackCount       Same rack count.
     * @param diffRackCount       Different rack count.
     * @param diffDataCenterCount Different data center count.
     * @param count               Count.
     * @param dataCenter          Data center.
     * @param ttl                 Time to live.
     * @throws IOException IOException Http connection is fail or server response
     *                     within some error message.
     */
    public void preAllocateVolumes(int sameRackCount, int diffRackCount, int diffDataCenterCount, int count,
                                   String dataCenter, String ttl) throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.preAllocateVolumes(new PreAllocateVolumesParams(
                String.valueOf(diffDataCenterCount) + String.valueOf(diffRackCount) + String.valueOf(sameRackCount),
                count, dataCenter, ttl));
    }

    /**
     * Fetch http API input stream cache.
     *
     * @param request Http request.
     * @return Stream fetch by http response.
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public StreamResponse fetchStreamCacheByRequest(Request request) throws IOException {
        Call call = null;
        Response response = null;
//        CloseableHttpResponse response = null;
        // request.setHeader("Connection", "close");
        StreamResponse cache = null;

        try {
            call = httpClient.newCall(request);
            response = call.execute();
            ResponseBody responseBody = response.body();
            if (responseBody != null) {
                cache = new StreamResponse(responseBody.byteStream(), response.code());
            }
//            response = httpClient.execute(request, HttpClientContext.create());
//            HttpEntity entity = response.getEntity();
//            cache = new StreamResponse(entity.getContent(), response.getStatusLine().getStatusCode());
//            EntityUtils.consume(entity);
        } finally {
//            if (call != null) {
//                call.cancel();
//            }
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            request = null;
//            request.releaseConnection();
        }
        return cache;
    }

    /**
     * Fetch http API hearers with status code(in array).
     *
     * @param request Only http method head.
     * @return Header fetch by http response.
     * @throws IOException Http connection is fail or server response within some
     *                     error message.
     */
    public HeaderResponse fetchHeaderByRequest(Request request) throws IOException {
        Call call = null;
        Response response = null;
//        CloseableHttpResponse response = null;
        // request.setHeader("Connection", "close");
        HeaderResponse headerResponse;

        try {
            call = httpClient.newCall(request);
            response = call.execute();
            Header[] myheaders = new Header[response.headers().size()];
            for (int i = 0; i < response.headers().size(); i++) {
                myheaders[i] = new Header(response.headers().name(i), response.headers().value(i));
            }
            headerResponse = new HeaderResponse(myheaders, response.code());
//            response = httpClient.execute(request, HttpClientContext.create());
//            headerResponse = new HeaderResponse(response.getAllHeaders(), response.getStatusLine().getStatusCode());
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            request = null;
//            request.releaseConnection();
        }
        return headerResponse;
    }


    /**
     * Thread for close expired connections.
     */
    private class IdleConnectionMonitorThread extends Thread {

        private volatile boolean shutdown;

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(statusExpiry);
                        // Close free connection
//                        clientConnectionManager.closeExpiredConnections();
//                        clientConnectionManager.closeIdleConnections(idleConnectionExpiry, TimeUnit.SECONDS);
//                        log.debug(
//                                "http client pool state [" + clientConnectionManager.getTotalStats().toString() + "]");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        void shutdown() {
            this.shutdown = true;
            this.interrupt();
            synchronized (this) {
                notifyAll();
            }
        }
    }
}
