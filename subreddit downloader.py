# Code modified from https://old.reddit.com/r/pushshift/comments/i8dlzs/how_to_download_all_posts_from_a_subreddit/g193tsq/

import requests
import time
from datetime import datetime, timezone
import csv, os

os.chdir('./data/pushshift')

# Get if we are downloading comments or posts
choice = input('Type of media [s]ubmission / [c]omment: ')
if choice.startswith('s'):
    mediaType = 'submission'
elif choice.startswith('c'):
    mediaType = 'comment'
else:
    raise ValueError('Media type must be submission or comment')

subreddit = 'tipofmyjoystick'
maxThings = 1000
printWait = 2
requestSize = 500


def requestJSON(url):
    while True:
        try:
            r = requests.get(url)
            if r.status_code != 200:
                print('error code', r.status_code)
                time.sleep(5)
                continue
            else:
                break
        except Exception as e:
            print(e)
            time.sleep(5)
            continue
    return r.json()


def downloadFromPushshift():
    meta = requestJSON('https://api.pushshift.io/meta')
    limitPerMinute = 60 # meta['server_ratelimit_per_minute']
    requestWait = 60 / limitPerMinute

    print('server_ratelimit_per_minute', limitPerMinute)

    i = 0
    with open(f'{subreddit}_{mediaType}_{str(int(time.time()))}.csv', 'w',
            newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        print(f'\n[starting {mediaType}s]')

        if maxThings < 0:

            url = f'https://api.pushshift.io/reddit/search/{mediaType}/?subreddit={subreddit}'\
                    + '&metadata=true&size=0'

            json = requestJSON(url)

            totalResults = json['metadata']['total_results']
            print(f'total {mediaType}s in {subreddit}: {totalResults}')
        else:
            totalResults = maxThings
            print('downloading most recent', maxThings)

        blacklistUsers = 'AutoModerator' + \
            (',[deleted]' if mediaType == 'comment' else '')

        startTime = time.time()
        timePrint = startTime
        
        created_utc = '2d' # To get a more representative sample, we exclude recent posts within 2 days
        
        while True:
            url = f'http://api.pushshift.io/reddit/search/{mediaType}/?subreddit={subreddit}'\
                    + '&size=' + str(requestSize)\
                    + '&before=' + str(created_utc)\
                    + '&author=!' + blacklistUsers\
                    + '&mod_removed=0&sort=desc&sort_type=created_utc&fields=id,title,author,created_utc,removed_by_category'

            json = requestJSON(url)

            if len(json['data']) == 0:
                break

            doneHere = False
            for item in json['data']:
                created_utc = item["created_utc"]
                if 'removed_by_category' not in item:
                    if mediaType == 'submission':
                        writer.writerow([item['id']])
                    elif mediaType == 'comment':
                        commentId = item['id']
                        user = item['author']
                        timeCreated = datetime.fromtimestamp(item['created_utc'], timezone.utc).isoformat()
                        
                        writer.writerow([
                            commentId, 
                            user, 
                            timeCreated,
                            ])
                i += 1
                if i >= totalResults:
                    doneHere = True
                    break

            if doneHere:
                break

            if time.time() - timePrint > printWait:
                timePrint = time.time()
                percent = i / totalResults * 100

                timePassed = time.time() - startTime

                print('{:.2f}'.format(percent) + '%', '|',
                        time.strftime("%H:%M:%S", time.gmtime(timePassed)))

            time.sleep(requestWait)


downloadFromPushshift()
