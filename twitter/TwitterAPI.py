import pandas as pd 
import tweepy
import time

class TwitterAPI():
    def __init__(self,
                 api=None,
                 user_name = "user_name"):
        self.api = api
        self.user_name = user_name
    
    def GetStatus(self,since_id=None):
        """
        This is the function we used to get all the tweets from the 
        user's timeline
        """
        if since_id:
            return tweepy.Cursor(self.api.user_timeline,id =self.user_name,since_id= since_id).items()
        else :
            return tweepy.Cursor(self.api.user_timeline,id =self.user_name).items()
    
    def GetComment(self,since_id=None):
        """
        This is the function we used to get all the tweets from the 
        user's timeline
        """
        return tweepy.Cursor(self.api.search, q='to:{}'.format(self.user_name),
                            tweet_mode='extended').items()
    
    def cleanTweetDict(self,t):
        '''
        This is the Function to clean the Tweets
        '''
        attr = ['id','created_at','text','retweet_count','favorite_count',
                'favorited','retweeted','lang','geo','coordinates','place']

        d_t= {}
        for key in t.keys():
            if key in attr:
                d_t[key] = t[key]

            if key == 'entities':
                d_t['hashtags'] = t[key]['hashtags']
                d_t['user_mentions'] = t[key]['user_mentions']
        return d_t

    def Iterator(self,itr,tweet_id = 121):
        '''
        This is the function to get the comments on a tweet
        '''
        tweets = []
        while True:
            try:
                tweet = itr.next()
                if not hasattr(tweet, '_json'):
                    continue
                if tweet._json:
                    comment = self.GetComment()
                    twt = self.cleanTweetDict(tweet._json)
                    twt['comments_metadata'] = self.IteratorComments(comment,tweet._json['id'])
                    tweets.append(twt)

            except tweepy.RateLimitError as e:
                print("Twitter api rate limit reached".format(e))
                time.sleep(60)
                continue

            except tweepy.TweepError as e:
                print("Tweepy error occured:{}".format(e))
                break

            except StopIteration:
                break

            except Exception as e:
                print("Failed while fetching replies {}".format(e))
                break
        return tweets

    def IteratorComments(self,itr,tweet_id = 121):
        '''
        This is the function to get the comments on a tweet
        '''
        comments = []
        while True:
            t_d = {}
            try:
                reply = itr.next()
                if not hasattr(reply, 'in_reply_to_status_id_str'):
                    continue
                if reply.in_reply_to_status_id == tweet_id:
                    t_d['text'] = reply._json['full_text']
                    t_d['geo'] = reply._json['geo']
                    t_d['coordinates'] = reply._json['coordinates']
                    comments.append(t_d)

            except tweepy.RateLimitError as e:
                print("Twitter api rate limit reached".format(e))
                time.sleep(60)
                continue

            except tweepy.TweepError as e:
                print("Tweepy error occured:{}".format(e))
                break

            except StopIteration:
                break

            except Exception as e:
                print("Failed while fetching replies {}".format(e))
                break
        
        return comments
    


    def IteratorUser(self,itr,tweet_id = 121):
        '''
        This is the function to get the comments on a tweet
        '''
        try:
            reply = itr.next()
            if reply._json:
                user = reply._json['user']
            else:
                raise Exception("No Tweet Found for the Account")

        except tweepy.RateLimitError as e:
            print("Twitter api rate limit reached".format(e))
            time.sleep(60)
            user = self.IteratorUser(itr)
        except tweepy.TweepError as e:
            print("Tweepy error occured:{}".format(e))

        return user