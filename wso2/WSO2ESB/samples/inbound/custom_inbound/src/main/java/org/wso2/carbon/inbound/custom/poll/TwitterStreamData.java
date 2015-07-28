/**
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.custom.poll;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class TwitterStreamData extends GenericPollingConsumer{

    private static final Log log = LogFactory.getLog(TwitterStreamData.class);

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecret;
    Properties twitterProperties;

    private Queue<Status> twitterQueue;

    private String[] filterTracks;
    private String[] filterLanguages;
    private String[] filterFollowTemp;
    private long[] filterFollow;

    private int count;
    private String follow;
    private String track;
    private String locations;
    private String language;
    private String filterLevel;

    private String injectingSeq;


    public TwitterStreamData(Properties twitterProperties, String name,
                               SynapseEnvironment synapseEnvironment, long scanInterval,
                               String injectingSeq, String onErrorSeq, boolean coordination,
                               boolean sequential) {

        super(twitterProperties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        log.info("Initialized the Twitter polling consumer.");
        this.injectingSeq=injectingSeq;
        loadCredentials(twitterProperties);
        loadRequestParameters(twitterProperties);
        this.filterTracks = track.split(",");
        this.filterLanguages=language.split(",");
        if(follow!=null) {
            this.filterFollowTemp = follow.split(",");
            filterFollow = new long[filterFollowTemp.length];
            for (int i = 0; i < filterFollowTemp.length; i++) {
                filterFollow[i] = Long.parseLong(filterFollowTemp[i]);
            }
        }
        twitterQueue = new LinkedList<Status>();

        //Establishing connection with twitter streaming api
        setupConnection();
        log.info("Twitter connection setup successfully.");
    }

    @Override
    public Object poll() {
        try {
            while (twitterQueue.size() > 0) {
                Status status = twitterQueue.poll();
                if (injectingSeq != null) {
                    injectMessage(status.toString(), TwitterConstant.CONTENT_TYPE);
                    log.debug("injecting twitter message to the sequence : " + injectingSeq);
                }else
                {
                    log.error("Sequence: " + injectingSeq + " not found");
                }
            }
        }
        catch (Exception e) {
            log.error("Error while receiving Twitter message. " + e.getMessage(),e);
        }
        return  null;

    }
    /**
     * Load credentials from the Twitter end-point property file.
     * @param properties
     */
    private void loadCredentials(Properties properties) {
        this.consumerKey = properties.getProperty(TwitterConstant.CONSUMER_KEY);
        this.consumerSecret = properties
                .getProperty(TwitterConstant.CONSUMER_SECRET);
        this.accessSecret = properties
                .getProperty(TwitterConstant.ACCESS_SECRET);
        this.accessToken = properties.getProperty(TwitterConstant.ACCESS_TOKEN);
    }

    /**
     * Load credentials from the Twitter end-point property file.
     * @param properties
     */
    private void loadRequestParameters(Properties properties) {
        if (properties.getProperty(TwitterConstant.FILTER_COUNT)!=null)
        {
            this.count = Integer.parseInt(properties.getProperty(TwitterConstant.FILTER_COUNT));
        }
        this.follow = properties.getProperty(TwitterConstant.FILTER_FOLLOW);
        this.track = properties.getProperty(TwitterConstant.FILTER_TRACK);
        this.locations = properties.getProperty(TwitterConstant.FILTER_LOCATIONS);
        this.language = properties.getProperty(TwitterConstant.FILTER_LANGUAGE);
        this.filterLevel = properties.getProperty(TwitterConstant.FILTER_FILTER_LEVEL);
    }

    /**
     * Setting up a connection with Twitter Stream API with the given credentials
     */
    private void setupConnection() {
        StatusListener listener = new StatusListenerImpl();

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessSecret);
        TwitterStream twitterStream = new TwitterStreamFactory(
                configurationBuilder.build()).getInstance();
        twitterStream.addListener(listener);
        FilterQuery query = new FilterQuery();
        if (filterLanguages!=null) {
            query.language(filterLanguages);
        }
        if (filterTracks!=null)
        {
            query.track(filterTracks);
        }
        if (follow!=null)
        {
            query.follow(filterFollow);
        }
        if (filterLevel!=null) {
            query.filterLevel(filterLevel);
        }
        query.count(count);
        twitterStream.filter(query);
    }

    /**
     * Twitter Stream Listener Impl
     * onStatus will invoke whenever new twitter come.
     * New Twitter is store in queue until it is inbound-endpoint poll
     *
     */
    class StatusListenerImpl implements StatusListener {
        public void onStatus(Status status) {
            if (status.getRetweetedStatus() == null) {
                twitterQueue.add(status);
            }
        }

        public void onException(Exception ex) {
            log.error("Twitter source threw an exception", ex);
        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            log.debug("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
        }

        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            log.debug("Got track limitation notice: " +numberOfLimitedStatuses);
        }

        public void onScrubGeo(long userId, long upToStatusId) {
            log.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
        }

        public void onStallWarning(StallWarning warning) {
            log.debug("Got stall warning:" + warning);
        }

    }

}
