import praw
import csv, os
from datetime import datetime, timezone

"""
https://api.pushshift.io/reddit/submission/search?subreddit=tipofmyjoystick&limit=10&mod_removed=0&sort=desc&sort_type=created_utc
https://api.pushshift.io/reddit/submission/search?subreddit=tipofmyjoystick&metadata=true&size=0
"""

reddit = praw.Reddit("scraper")
READFILE = './pushshift/tipofmyjoystick_submission_1641025476.csv'
WRITEFILE = 'posts.csv'

os.chdir('./data/')

FORMAT = [
    'Platform(s):',
    'Genre:',
    'Estimated year of release:',
    'Graphics/art style:',
    'Notable characters:',
    'Notable gameplay mechanics:',
    'Other details:'
]

def listNewFlairs():
    sub = reddit.subreddit('tipofmyjoystick')

    for submission in sub.new(limit=10):
        print(submission.link_flair_text)
    

def getFlairsFromFile(filename):
    i = 0
    maxNum = 1000
    flairs = []
    
    with open(filename) as f:
        while ((nextId := f.readline().strip()) != '') and (i <= maxNum):
            i += 1
            submission = reddit.submission(id=nextId)
            flair = submission.link_flair_text
            flairs.append(flair)
            print(nextId, flair)


# Reads a file list of post IDs and 
def getIDsFromFile(readfile, idType):
    if idType == 'post':
        prefix = 't3_'
    elif idType == 'comment':
        prefix = 't1_'
    
    with open(readfile) as f:
        while (id := f.readline().strip()) != '':
            yield prefix+id


def getDataFromFile(readfile, writefile, expectedLines=0):
    lines = 0
    onePercentRead = 0
    if expectedLines != 0:
        onePercentRead = expectedLines / 100
    
    with open(writefile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        for submission in reddit.info(getIDsFromFile(readfile, 'post')):
            lines += 1
            
            # We don't want to include mod announcements
            if submission.distinguished:
                continue
            # We don't want to include deleted/removed posts
            if submission.removed_by_category is not None:
                continue
            
            # Flair/solved
            isSolved = True
            if (flair := submission.link_flair_text) is None:
                flair = ''
                isSolved = False
            flair = flair.replace("’", "'") # sanitize
            
            # Author
            if submission.author is not None:
                user = submission.author.name
            else:
                user = ''
            
            # Does the post follow the format
            followsFormat = True
            for part in FORMAT:
                if part not in submission.selftext:
                    followsFormat = False
                    break
            
            # Others
            title = submission.title.replace("’", "'")
            date = datetime.fromtimestamp(submission.created_utc, timezone.utc).isoformat()
            score = submission.score
            commentNum = submission.num_comments
            isNSFW = submission.over_18
            submissionId = submission.id
            postLength = len(submission.selftext)
            bodyText = submission.selftext
            
            # We check again for deleted/removed posts
            if bodyText == '[deleted]' or bodyText == '[removed]':
                continue
            
            # Finally, writing to csv
            writer.writerow([
                submissionId,
                title, 
                user, 
                bodyText,
                flair, 
                date, 
                score, 
                commentNum, 
                isNSFW, 
                postLength,
                followsFormat,
                isSolved,
            ])
            
            # Check if we've advanced a percent
            if onePercentRead != 0:
                percent = int(lines / expectedLines * 100)
                prevPercent = int((lines-1) / expectedLines * 100)
                if percent > prevPercent:
                    print(f'{percent}% done')
        
        print('Total rows: ', lines)


# Removes null bytes, overwriting the file
def sanitizeFile(filename):
    with open(filename, 'rb') as f:
        data = f.read()

    with open(filename, 'wb') as f:
        f.write(data.replace(b'\x00', b''))
    

# Dramatically reduces file size by removing the body text field
def pareBodytextFromFile(readfile, writefile):
    with open(readfile, newline='', encoding='utf-8') as infile, \
        open(writefile, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        for row in reader:
            writer.writerow(row[:3] + [''] + row[4:])


getDataFromFile(READFILE, WRITEFILE, expectedLines=160000)
sanitizeFile(WRITEFILE)
pareBodytextFromFile(WRITEFILE, 'posts-pared.csv')