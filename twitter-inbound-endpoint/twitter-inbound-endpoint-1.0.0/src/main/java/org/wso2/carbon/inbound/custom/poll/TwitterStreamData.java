/*
 *  Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.custom.poll;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class TwitterStreamData extends GenericPollingConsumer {

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
    private String countParam;

    private int count;
    private String follow;
    private String track;
    private String locations;
    private String language;

    private String injectingSeq;

    public TwitterStreamData(Properties twitterProperties, String name,
                             SynapseEnvironment synapseEnvironment, long scanInterval,
                             String injectingSeq, String onErrorSeq, boolean coordination,
                             boolean sequential) {

        super(twitterProperties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        log.info("Initialized the Twitter polling consumer.");
        this.injectingSeq = injectingSeq;
        loadCredentials(twitterProperties);
        loadRequestParameters(twitterProperties);
        if (track != null) {
            this.filterTracks = track.split(",");
        }
        if (filterLanguages != null) {
            this.filterLanguages = language.split(",");
        }
        if (follow != null) {
            this.filterFollowTemp = follow.split(",");
            filterFollow = new long[filterFollowTemp.length];
            for (int i = 0; i < filterFollowTemp.length; i++) {
                filterFollow[i] = Long.parseLong(filterFollowTemp[i]);
            }
        }
        if (countParam != null) {
            try {
                this.count = Integer.parseInt(countParam);
            } catch (NumberFormatException nfe) {
                log.error("the count should be a number.");
            }
        }
        twitterQueue = new LinkedList<Status>();

        //Establishing connection with twitter streaming api
        try {
            setupConnection();
            log.info("Twitter connection setup successfully created.");
        } catch (TwitterException e) {
            log.error("Error while set the twitter connection. " + e.getMessage(), e);
        }

    }

    @Override
    public Object poll() {
        try {
            while (twitterQueue.size() > 0) {
                Status status = twitterQueue.poll();
                if (injectingSeq != null) {
                    injectMessage(status.toString(), TwitterConstant.CONTENT_TYPE);
                    if (log.isDebugEnabled()) {
                        log.debug("injecting twitter message to the sequence : " + injectingSeq);
                    }
                } else {
                    log.error("Sequence: " + injectingSeq + " not found");
                }
            }
        } catch (Exception e) {
            log.error("Error while receiving Twitter message. " + e.getMessage(), e);
        }
        return null;

    }

    /**
     * Load credentials from the Twitter inbound endpoint file.
     *
     * @param properties the twitter properties
     */
    private void loadCredentials(Properties properties) {
        this.consumerKey = properties.getProperty(TwitterConstant.CONSUMER_KEY);
        this.consumerSecret = properties
                .getProperty(TwitterConstant.CONSUMER_SECRET);
        this.accessSecret = properties
                .getProperty(TwitterConstant.ACCESS_SECRET);
        this.accessToken = properties.getProperty(TwitterConstant.ACCESS_TOKEN);
        if (log.isDebugEnabled()) {
            log.debug("Loading the twitter consumerKey : " + consumerKey + ",consumerSecret : " + consumerSecret + ",accessToken : " + accessToken + ",accessSecret : " + accessSecret);
        }
    }

    /**
     * Load the parameters from the Twitter inbound endpoint file.
     *
     * @param properties the twitter properties
     */
    private void loadRequestParameters(Properties properties) {
        this.countParam = properties.getProperty(TwitterConstant.FILTER_COUNT);
        this.follow = properties.getProperty(TwitterConstant.FILTER_FOLLOW);
        this.track = properties.getProperty(TwitterConstant.FILTER_TRACK);
        this.locations = properties.getProperty(TwitterConstant.FILTER_LOCATIONS);
        this.language = properties.getProperty(TwitterConstant.FILTER_LANGUAGE);
        if (log.isDebugEnabled()) {
            log.debug("Loading the twitter URL paramters. countParam: " + countParam + ",follow : " + follow + ",track : " + track + ",locations : " + locations + ",language : " + language);
        }
    }

    /**
     * Setting up a connection with Twitter Stream API with the given credentials
     */
    private void setupConnection() throws TwitterException {

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessSecret);

        StatusListener statusStreamslistener;
        SiteStreamsListener siteStreamslistener;
        TwitterStream twitterStream = new TwitterStreamFactory(
                configurationBuilder.build()).getInstance();
        String twitterOperation=properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION);
        if (twitterOperation.equals(TwitterConstant.FILTER_STREAM_OPERATION) ||twitterOperation.equals(TwitterConstant.FIREHOSE_STREAM_OPERATION) ||
                twitterOperation.equals(TwitterConstant.LINK_STREAM_OPERATION) || twitterOperation.equals(TwitterConstant.SAMPLE_STREAM_OPERATION) || twitterOperation.equals(TwitterConstant.USER_STREAM_OPERATION)) {
            statusStreamslistener = new StatusListenerImpl();
            twitterStream.addListener(statusStreamslistener);
        } else if (twitterOperation.equals(TwitterConstant.SITE_STREAM_OPERATION)) {
            siteStreamslistener = new siteStreamsListenerImpl();
            twitterStream.addListener(siteStreamslistener);
        } else {
            log.error("The operation :"+ twitterOperation + " not found" );
        }

        /*Returns public statuses that match one or more filter predicates.At least a follow or userId must be specified. Multiple parameters may be specified.
        The default access level allows up to 400 track keywords, 5,000 follow userids and 25 0.1-360 degree location boxes.
        */
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.FILTER_STREAM_OPERATION)) {
            FilterQuery query = new FilterQuery();
            if (filterLanguages != null) {
                query.language(filterLanguages);
            }
            if (filterTracks != null) {
                query.track(filterTracks);
            }
            if (follow != null) {
                query.follow(filterFollow);
            }
            query.count(count);
            if (query!=null)
            {
            twitterStream.filter(query);
        }
        /* Returns a small random sample of all public statuses.*/
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.SAMPLE_STREAM_OPERATION)) {

            if (filterLanguages != null) {
                twitterStream.sample(language);
            }
            twitterStream.sample();
        }
        /*Returns all public statuses. Few applications require this level of access.
         Creative use of a combination of other resources and various access levels can satisfy nearly every application use case.
         This endpoint requires special permission to access.
        */
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.FIREHOSE_STREAM_OPERATION)) {
            if (countParam != null) {
                twitterStream.firehose(count);
            }
        }
        /*User Streams provide a stream of data and events specific to the authenticated user.This provides to access the Streams messages for a single user.
        */
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.USER_STREAM_OPERATION)) {
            if (filterTracks != null) {
                twitterStream.user(track);
            }
            twitterStream.user();
        }
        /*User Streams provide a stream of data and events specific to the authenticated user.This provides to access the Streams messages for a single user.
        */
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.RETWEET_STREAM_OPERATION)) {
            twitterStream.retweet();
        }
        /*Site Streams allows services, such as web sites or mobile push services, to receive real-time updates for a large number of users.
         Events may be streamed for any user who has granted OAuth access to your application. Desktop applications or applications with few users should use user streams.
        */
        if ((properties.getProperty(TwitterConstant.TWITTER_STREAM_OPERATION)).equals(TwitterConstant.SITE_STREAM_OPERATION)) {

        }
    }

    /**
     * Twitter Stream Listener Impl
     * onStatus will invoke whenever new twitter come.
     * New Twitter is store in queue until it is inbound-endpoint poll
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
            log.debug("Got track limitation notice: " + numberOfLimitedStatuses);
        }

        public void onScrubGeo(long userId, long upToStatusId) {
            log.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
        }

        public void onStallWarning(StallWarning warning) {
            log.debug("Got stall warning:" + warning);
        }

    }

    class siteStreamsListenerImpl implements SiteStreamsListener {
        @Override
        public void onStatus(long forUser, Status status) {
            log.debug("onStatus for_user:" + forUser + " @" + status.getUser().getScreenName() + " - " + status.getText());
        }

        @Override
        public void onDeletionNotice(long forUser, StatusDeletionNotice statusDeletionNotice) {
            log.debug("Got a status deletion notice for_user:"
                    + forUser + " id:" + statusDeletionNotice.getStatusId());
        }

        @Override
        public void onFriendList(long forUser, long[] friendIds) {
            log.debug("onFriendList for_user:" + forUser);
            for (long friendId : friendIds) {
                log.debug(" " + friendId);
            }
            log.debug("");
        }

        @Override
        public void onFavorite(long forUser, User source, User target, Status favoritedStatus) {
            log.debug("onFavorite for_user:" + forUser + " source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + favoritedStatus.getUser().getScreenName() + " - "
                    + favoritedStatus.getText());
        }

        @Override
        public void onUnfavorite(long forUser, User source, User target, Status unfavoritedStatus) {
            log.debug("onUnFavorite for_user:" + forUser + " source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + unfavoritedStatus.getUser().getScreenName()
                    + " - " + unfavoritedStatus.getText());
        }

        @Override
        public void onFollow(long forUser, User source, User followedUser) {
            log.debug("onFollow for_user:" + forUser + " source:@"
                    + source.getScreenName() + " target:@"
                    + followedUser.getScreenName());
        }

        @Override
        public void onUnfollow(long forUser, User source, User followedUser) {
            log.debug("onUnfollow for_user:" + forUser + " source:@"
                    + source.getScreenName() + " target:@"
                    + followedUser.getScreenName());
        }

        @Override
        public void onDirectMessage(long forUser, DirectMessage directMessage) {
            log.debug("onDirectMessage for_user:" + forUser + " text:"
                    + directMessage.getText());
        }

        @Override
        public void onDeletionNotice(long forUser, long directMessageId, long userId) {
            log.debug("Got a direct message deletion notice for_user:"
                    + forUser + " id:" + directMessageId);
        }

        @Override
        public void onUserListMemberAddition(long forUser, User addedMember, User listOwner, UserList list) {
            log.debug("onUserListMemberAddition for_user:" + forUser
                    + " member:@" + addedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListMemberDeletion(long forUser, User deletedMember, User listOwner, UserList list) {
            log.debug("onUserListMemberDeletion for_user:" + forUser
                    + " member:@" + deletedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListSubscription(long forUser, User subscriber, User listOwner, UserList list) {
            log.debug("onUserListSubscribed for_user:" + forUser
                    + " subscriber:@" + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListUnsubscription(long forUser, User subscriber, User listOwner, UserList list) {
            log.debug("onUserListUnsubscribed for_user:" + forUser
                    + " subscriber:@" + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListCreation(long forUser, User listOwner, UserList list) {
            log.debug("onUserListCreated for_user:" + forUser
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListUpdate(long forUser, User listOwner, UserList list) {
            log.debug("onUserListUpdated for_user:" + forUser
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListDeletion(long forUser, User listOwner, UserList list) {
            log.debug("onUserListDestroyed for_user:" + forUser
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserProfileUpdate(long forUser, User updatedUser) {
            log.debug("onUserProfileUpdated for_user:" + forUser
                    + " user:@" + updatedUser.getScreenName());
        }

        @Override
        public void onUserDeletion(long forUser, long deletedUser) {
            log.debug("onUserDeletion for_user:" + forUser
                    + " user:@");
        }

        @Override
        public void onUserSuspension(long forUser, long suspendedUser) {
            log.debug("onUserSuspension for_user:" + forUser
                    + " user:@" + suspendedUser);
        }

        @Override
        public void onBlock(long forUser, User source, User blockedUser) {
            log.debug("onBlock for_user:" + forUser
                    + " source:@" + source.getScreenName()
                    + " target:@" + blockedUser.getScreenName());
        }

        @Override
        public void onUnblock(long forUser, User source, User unblockedUser) {
            log.debug("onUnblock for_user:" + forUser
                    + " source:@" + source.getScreenName()
                    + " target:@" + unblockedUser.getScreenName());
        }

        @Override
        public void onRetweetedRetweet(User source, User target, Status retweetedStatus) {
            log.debug("onRetweetedRetweeted source:" + source.getScreenName()
                    + " target:@" + target.getScreenName()
                    + " retweetedStatus:@" + retweetedStatus.getUser().getScreenName() + " - "
                    + retweetedStatus.getText());
        }

        @Override
        public void onFavoritedRetweet(User source, User target, Status favoritedStatus) {
            log.debug("onFavoritedRetweet source:" + source.getScreenName()
                    + " target:@" + target.getScreenName()
                    + " favoritedStatus:@" + favoritedStatus.getUser().getScreenName() + " - "
                    + favoritedStatus.getText());
        }

        @Override
        public void onDisconnectionNotice(String line) {
            log.debug("onDisconnectionNotice:" + line);
        }

        @Override
        public void onException(Exception ex) {
            log.error("Twitter source threw an exception", ex);
        }


    }

    class UserStreamListenerImpl implements UserStreamListener {
        @Override
        public void onStatus(Status status) {
            log.debug("onStatus @" + status.getUser().getScreenName() + " - " + status.getText());
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            log.debug("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
        }

        @Override
        public void onDeletionNotice(long directMessageId, long userId) {
            log.debug("Got a direct message deletion notice id:" + directMessageId);
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            log.debug("Got a track limitation notice:" + numberOfLimitedStatuses);
        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
            log.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
        }

        @Override
        public void onStallWarning(StallWarning warning) {
            log.debug("Got stall warning:" + warning);
        }

        @Override
        public void onFriendList(long[] friendIds) {
            System.out.print("onFriendList");
            for (long friendId : friendIds) {
                System.out.print(" " + friendId);
            }
            log.debug("");
        }

        @Override
        public void onFavorite(User source, User target, Status favoritedStatus) {
            log.debug("onFavorite source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + favoritedStatus.getUser().getScreenName() + " - "
                    + favoritedStatus.getText());
        }

        @Override
        public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
            log.debug("onUnFavorite source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + unfavoritedStatus.getUser().getScreenName()
                    + " - " + unfavoritedStatus.getText());
        }

        @Override
        public void onFollow(User source, User followedUser) {
            log.debug("onFollow source:@"
                    + source.getScreenName() + " target:@"
                    + followedUser.getScreenName());
        }

        @Override
        public void onUnfollow(User source, User followedUser) {
            log.debug("onFollow source:@"
                    + source.getScreenName() + " target:@"
                    + followedUser.getScreenName());
        }

        @Override
        public void onDirectMessage(DirectMessage directMessage) {
            log.debug("onDirectMessage text:"
                    + directMessage.getText());
        }

        @Override
        public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {
            log.debug("onUserListMemberAddition added member:@"
                    + addedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {
            log.debug("onUserListMemberDeleted deleted member:@"
                    + deletedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListSubscription(User subscriber, User listOwner, UserList list) {
            log.debug("onUserListSubscribed subscriber:@"
                    + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {
            log.debug("onUserListUnsubscribed subscriber:@"
                    + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListCreation(User listOwner, UserList list) {
            log.debug("onUserListCreated  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListUpdate(User listOwner, UserList list) {
            log.debug("onUserListUpdated  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserListDeletion(User listOwner, UserList list) {
            log.debug("onUserListDestroyed  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        @Override
        public void onUserProfileUpdate(User updatedUser) {
            log.debug("onUserProfileUpdated user:@" + updatedUser.getScreenName());
        }

        @Override
        public void onUserDeletion(long deletedUser) {
            log.debug("onUserDeletion user:@" + deletedUser);
        }

        @Override
        public void onUserSuspension(long suspendedUser) {
            log.debug("onUserSuspension user:@" + suspendedUser);
        }

        @Override
        public void onBlock(User source, User blockedUser) {
            log.debug("onBlock source:@" + source.getScreenName()
                    + " target:@" + blockedUser.getScreenName());
        }

        @Override
        public void onUnblock(User source, User unblockedUser) {
            log.debug("onUnblock source:@" + source.getScreenName()
                    + " target:@" + unblockedUser.getScreenName());
        }

        @Override
        public void onRetweetedRetweet(User source, User target, Status retweetedStatus) {
            log.debug("onRetweetedRetweet source:@" + source.getScreenName()
                    + " target:@" + target.getScreenName()
                    + retweetedStatus.getUser().getScreenName()
                    + " - " + retweetedStatus.getText());
        }

        @Override
        public void onFavoritedRetweet(User source, User target, Status favoritedRetweet) {
            log.debug("onFavroitedRetweet source:@" + source.getScreenName()
                    + " target:@" + target.getScreenName()
                    + favoritedRetweet.getUser().getScreenName()
                    + " - " + favoritedRetweet.getText());
        }

        @Override
        public void onQuotedTweet(User source, User target, Status quotingTweet) {
            log.debug("onQuotedTweet" + source.getScreenName()
                    + " target:@" + target.getScreenName()
                    + quotingTweet.getUser().getScreenName()
                    + " - " + quotingTweet.getText());
        }

        @Override
        public void onException(Exception ex) {
            log.error("onException:" + ex.getMessage());
        }

    }

}
