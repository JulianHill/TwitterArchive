




package twitter_loop;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;


import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;



public class Twitter_loop_streaming {




    private ConfigurationBuilder cb;
    private DB db;
    private DBCollection items;




    /**
     * static block used to construct a connection with tweeter with twitter4j
     * configuration with provided settings. This configuration builder will be
     * used for next search action to fetch the tweets from twitter.com.
     */

    public static void main(String[] args) throws InterruptedException {


        Twitter_loop_streaming stream = new Twitter_loop_streaming();

        stream.loadMenu();

    }

    public void loadMenu() throws InterruptedException {


        System.out.print("Please choose a name for your stream:\t");


       Scanner input = new Scanner(System.in);
        String keyword = input.nextLine();


       connectdb(keyword);


            cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true);
            cb.setOAuthConsumerKey("XXX");
            cb.setOAuthConsumerSecret("XXX");
            cb.setOAuthAccessToken("XXX");
            cb.setOAuthAccessTokenSecret("XXX");

            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            StatusListener listener = new StatusListener() {

                public void onStatus(Status status) {
                    System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());

                    BasicDBObject basicObj = new BasicDBObject();
                    basicObj.put("user_name", status.getUser().getScreenName());
                    basicObj.put("retweet_count", status.getRetweetCount());
                    basicObj.put("tweet_followers_count", status.getUser().getFollowersCount());
                    basicObj.put("source",status.getSource());
                    //basicObj.put("coordinates",tweet.getGeoLocation());


                    UserMentionEntity[] mentioned = status.getUserMentionEntities();
                    basicObj.put("tweet_mentioned_count", mentioned.length);
                    basicObj.put("tweet_ID", status.getId());
                    basicObj.put("tweet_text", status.getText());



                    try {
                        items.insert(basicObj);
                    } catch (Exception e) {
                        System.out.println("MongoDB Connection Error : " + e.getMessage());

                    }


                }

                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                public void onScrubGeo(long userId, long upToStatusId) {
                    System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning stallWarning) {
                    //To change body of implemented methods use File | Settings | File Templates.
                }

                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };

            FilterQuery fq = new FilterQuery();
            String keywords[] = {"Germany"};

            fq.track(keywords);

            twitterStream.addListener(listener);
            twitterStream.filter(fq);

        }



    public  void connectdb(String keyword)
    {
        try {
            // on constructor load initialize MongoDB and load collection
            initMongoDB();
            items = db.getCollection(keyword);


            //make the tweet_ID unique in the database
            BasicDBObject index = new BasicDBObject("tweet_ID", 1);
            items.ensureIndex(index, new BasicDBObject("unique", true));

        } catch (MongoException ex) {
            System.out.println("MongoException :" + ex.getMessage());
        }

    }


    /**
     * initMongoDB been called in constructor so every object creation this
     * initialize MongoDB.
     */
    public void initMongoDB() throws MongoException {
        try {
            System.out.println("Connecting to Mongo DB..");
            Mongo mongo;
            mongo = new Mongo("127.0.0.1");
            db = mongo.getDB("tweetDB2");
        } catch (UnknownHostException ex) {
            System.out.println("MongoDB Connection Error :" + ex.getMessage());
        }
    }




}
