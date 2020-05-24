import socket
import sys
import requests
import requests_oauthlib
import json
ACCESS_TOKEN = '865262652829859841-PX9WsUVZym1etBwKG7EWxzuzfMaIRt0'
ACCESS_SECRET = 'XXQUtMPHrCFL2mfU7F06KXIkuX3u77JCMIH2LpoVjhERU'
CONSUMER_KEY = 'BIiHJ26woS89RZZzv2gcp2pmj'
CONSUMER_SECRET = 'ExtQD5Pv6iUpnI9esKxJFkdjamszjXiXSxWrwQCffdngsSfwfj'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN,
ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    #query_data = [('language', 'en'), ('locations', '34.5083, 16.2612, 55.6667, 32.1735'),('track','كورونا')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    print("start getting tweets")
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(bytes("{}\n".format(tweet_text), "utf-8"))
            #tcp_connection.send(tweet_text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 17017
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
print(resp)
send_tweets_to_spark(resp, conn)
