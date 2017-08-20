#!/usr/bin/python
import sys
import time
from datetime import datetime
import string
import urllib
from bs4 import BeautifulSoup 
import MySQLdb
from Review import Review
from Reviewer import Reviewer

# What this program does.
# - This program scrapes the following data from from site TripAdvisor.com and inserts them into mySql tables.
#   - review  data 

def ScrapeTripAdvisorHotelPages(onlyNewReviews):
    # this function will manipulate the url to navigate to the next page if it exists
    # the hotel name will either be pulled from a pre-compiled list of tripadvisor.com NYC hotels
    # or by starting from the url for tripadvisor.com NYC hotels, iterating through this url getting the hotels 
    # and then crawling to the individual hotel reviews page    
      
    startTime = time.time()  
    inPath = "/home/yourName/hotelUrls.txt"  
    inFile = open(inPath, "r")     # read in the hotel url's text file and loop thru
    ts = time.time()
    timeStamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
    path = "/home/yourName/Documents/Research/data/TripAdvisor.com_Reviews" + "_" + timeStamp
    logFile = open(path,"w")  # open the log file
    conn, cursor = OpenDBConn()  
    reviewIDs = GetReviewIDs(conn, cursor)
    reviewerIDs = GetReviewers(conn, cursor)
    res = 0
    today = datetime.now()
    
    for line in inFile:             
        # parse line and seed url
        lines = []
        delim = "-Reviews"
        lines = str(line).split(delim, 2)
        url_start = lines[0]+delim
        url_end = lines[1]
        url = url_start + url_end
         
        lines = str(url_end).split(',', 2) 
        #print lines[0], lines[1]
        skipFlag = (str(lines[1]).translate(string.maketrans("\n\t\r", "   "))).strip()
         
        # loop thru the hotel review pages
        firstReviewID = ""
        hotelName = ""
        runOnce = False
        URLGetError = False
        nextTenReviews = 0
        while(URLGetError == False):     
            # get url page to scrape
            try:
                if skipFlag == "skip":
                    break
                logFile.write("Getting URL " + url + "\n")
                print "Getting URL " + url 
                htmltext = urllib.urlopen(url).read()
            except:
                try:
                    # attempt a second time
                    htmltext = urllib.urlopen(url).read()
                except:
                    # failed to get URL twice, skip        
                    print "Skipping URL " + url
                    nextTenReviews += 10
                    url = url_start + "-or" + str(nextTenReviews) + url_end
                    print "Getting URL " + url   
                    continue 
            
            # place page into html parser
            soup = None
            soup = BeautifulSoup(htmltext,from_encoding="utf-8") 
            #print htmltext
            #print soup 
            
            # extract the reviews
            res, runOnce, firstReviewID, hotelName = ScrapeTripAdvisorHotelPage(onlyNewReviews, soup, url, logFile, runOnce, firstReviewID, hotelName, conn, cursor, reviewIDs, reviewerIDs, today)
            if res == -1:
                # all reviews processed, exit
                break
            # navigate to next page by manipulating the url, adding 10 to string "-orNN", between url_start and url_end 
            nextTenReviews += 10
            url = url_start + "-or" + str(nextTenReviews) + url_end
            if nextTenReviews == 700: # debug code 
                a = 0
                a = a
            
    # end process    
    conn.close()
    inFile.close()
    runTime = time.time() - startTime
    logFile.write('Run time of ' + str(runTime) + ' seconds') 
    print 'Run time of ' + str(runTime) + ' seconds'
    logFile.flush()
    logFile.close()        


def ScrapeTripAdvisorHotelPage(onlyNewReviews, soup, url, logFile, runOnce, firstReviewID, hotelName, conn, cursor, reviewIDs, reviewerIDs, today):

    if firstReviewID is "":
        # on the first page of reviews
        hotelName = GetHotelName(soup, logFile)  
       
    # hold reviews in a list for later insertion to database
    reviews = []
    reviewers = []
              
    # extracting individual reviews (this filters out advertised hotels with ratings, but not reviewed)
    while(True):
        # extract and wrap html body tags around the next review     
        try:
            i = 0
            for tag in soup.find_all('div', class_="reviewSelector "):
                review = ""
                i += 1
                # insert start page html tags
                review += "<html><head><title>ReviewNN</title></head><body>"
                review += str(tag)
                # insert end page html tags    
                review += "</body></html>"
                soupReviews = BeautifulSoup(review,from_encoding="utf-8")
                try:
                    res, runOnce, firstReviewID, review, reviewer = ScrapeTripAdvisorHotelReview(soup, soupReviews, url, logFile, runOnce, firstReviewID, hotelName, reviewIDs, reviewerIDs)             
                    if res == 0:
                        try:
                            if review != None:
                                reviews.append(review)
                        except:
                            errMsg = 'Error appending review to list.  Err. msg. = ' + str(sys.exc_info()[0]) + '\n'
                            logFile.write(errMsg)
                            print errMsg    
                        try:
                            if reviewer != None:
                                reviewers.append(reviewer)  
                        except:    
                            errMsg = 'Error appending reviewer to list.  Err. msg. = ' + str(sys.exc_info()[0]) + '\n'
                            logFile.write(errMsg)
                            print errMsg                                
                    else:
                        try:
                            # all hotel review pages processed, finish processing what is in the queue and go to next hotel 
                            if reviews:
                                InsertReviewsInDB(reviews, conn, cursor, logFile, reviewIDs)
                            if reviewers:
                                InsertReviewersInDB(reviewers, conn, cursor, logFile, reviewerIDs)
                        except:
                            errMsg = 'Error invoking inserts into db.  Err. msg. = ' + str(sys.exc_info()[0]) + '\n'
                            logFile.write(errMsg)
                            print errMsg                                
                        return -1, runOnce, firstReviewID, hotelName
                except:
                    errMsg = 'Error processing an individual review.  Err. msg. = ' + str(sys.exc_info()[0]) + '\n'
                    logFile.write(errMsg)
                    print errMsg                    
            else:     
                # no more reviews on page, break While loop
                break    
        except:
            msg = 'Error processing page of reviews ' + str(sys.exc_info()[0]) + '  Skipping review.' 
            print msg
            logFile.write(msg)
            
    InsertReviewsInDB(reviews, conn, cursor, logFile, reviewIDs)
    InsertReviewersInDB(reviewers, conn, cursor, logFile, reviewerIDs)
    return 0, runOnce, firstReviewID, hotelName    

def ScrapeTripAdvisorHotelReview(soup, soupReviews, url, logFile, runOnce, firstReviewID, hotelName, reviewIDs, reviewerIDs):
    # processing a single review, extract the items of interest
                
    # get review id test  
    try:
        reviewID = ""
        for tag in soupReviews.find_all('div', class_="reviewSelector "):
            reviewID = tag['id'].encode("ascii","ignore")
        # test if end of hotel ratings reached (it loops back to the first page, thus current review == first review)
        if runOnce == False:
            firstReviewID = reviewID
            runOnce = True
        else:
            if reviewID == firstReviewID:
                # all reviews processed, end processing
                return -1, runOnce, firstReviewID, None, None
    except:
        errMsg = 'Error fetching review id.  Null value used\n'
        logFile.write(errMsg)
        print errMsg

    # test if we already have the review, if so terminate hotel review extraction
    try:
        if reviewIDs.has_key(reviewID):
            if onlyNewReviews:
                # found a review already in the db, all following reviews will also be in our db, stop processing this hotel reviews
                msg = 'Extracted review found in our database, terminating review extraction process for this hotel\n'
                logFile.write(msg)
                print msg
                return -1, runOnce, firstReviewID, None, None
    except:
        errMsg = 'Error testing if reviewID exists in db.\n'
        logFile.write(errMsg)
        print errMsg

    # get review text
    try:
        reviewText = ""
        for tag in soupReviews.find_all('p', class_="partial_entry"):
            reviewText = (unicode(tag.contents[0].string)).encode('ascii','ignore')[:400]    
    except:
        errMsg = 'Error fetching review text.  Null value used.  Err. msg. = ' + str(sys.exc_info()[0]) +'\n'
        logFile.write(errMsg)
        print errMsg
        
    # test if a partnership collected review
    try:
        tagText = ''
        prtnrCollcted = 0
        for tag in soupReviews.find_all('p'):
            # get tag text 
            tagText = (unicode(tag.contents[0].string)).encode('ascii','ignore')[:31]
            if (tagText == 'Review collected in partnership'):
                prtnrCollcted = 1 
                break   
    except:
        errMsg = '' # not all reviews will be partner collected, no error condition exists    
                              
    # get review rating 
    try:
        rating = ''
        for tag in soupReviews.find_all('span', class_="rate sprite-rating_s rating_s"):
            #    
            # example - returns the following
            # - thus pull from row 2, cols 10,23, looks like I am forced to use a document object model 
            # <span class="rate sprite-rating_s rating_s">
            # <img alt="5 of 5 stars" class="sprite-rating_s_fill rating_s_fill s50" src="http://c1.tacdn.com/img2/x.gif">
            # </img></span> 
            #
            # place into a list and pull out the second tuple
            myTags = list(tag)
            tag = myTags[1]
            rating = tag['alt'].encode("ascii","ignore")
    except:
        errMsg = "Error fetching review rating.  Null value used.\n"
        logFile.write(errMsg)
        print errMsg 
            
    if (rating == ""):
        try:
            # second attempt
            for tag in soupReviews.find_all('span', class_="rate sprite-rating_cl_gry rating_cl_gry"):
                #    
                # example - returns the following
                # - thus pull from row 2, cols 10,23, looks like I am forced to use a document object model 
                # <span class="rate sprite-rating_s rating_s">
                # <img alt="5 of 5 stars" class="sprite-rating_s_fill rating_s_fill s50" src="http://c1.tacdn.com/img2/x.gif">
                # </img></span> 
                #
                # place into a list and pull out the second tuple
                myTags = list(tag)
                tag = myTags[1]
                rating = tag['alt'].encode("ascii","ignore")
                # Test TODO FIX once corrected remove comment out print statement
                #print "rating found is " + rating         
        except:
            errMsg = "Error fetching review rating.  Null value used.\n"
            logFile.write(errMsg)
            print errMsg 
            # useless review do not save     
        return 0, runOnce, firstReviewID, None, None

    # get stars numeric rating
    try:
        starsNum = 3
        delim = ' '
        lines = (rating.split(delim, 2))
        starsNum = float(lines[0]) 
    except:
        errMsg = 'Error transforming stars from string to numeric, defaulting to 3\n'
        logFile.write(errMsg)
        print errMsg     
    
    # transform star numeric rating to polarity measure
    try:
        polarity = 0
        if (starsNum < 3.0):
            polarity = -1
        elif (starsNum > 3.0):
            polarity = 1
        else:
            polarity = 0 
    except:
        errMsg = 'Error creating polarity score, defaulting to 0.\n'
        logFile.write(errMsg)
        print errMsg           
        
    # get review date 
    try:
        reviewDate = ""
        dateObj = None 
        # Two date variants can occur
        # - the first takes the form <span class="ratingDate relativeDate" title="June 4, 2014">Reviewed 4 weeks ago        
        for tag in soupReviews.find_all('span', class_="ratingDate relativeDate"):
            reviewDate = tag['title'].encode("ascii","ignore")
            # scan and replace commas with blanks
            reviewDate = string.replace(reviewDate, ",", "") 
            dateObj = datetime.strptime(reviewDate, '%B %d %Y')
        # test if review date found in previous step, if not try using a second date variant
        if (len(reviewDate) == 0):    
            # - the second takes the form <span class="ratingDate">Reviewed June 1, 2014   
            for tag in soupReviews.find_all('span', class_="ratingDate"):
                reviewDate = str(tag)
                # scan and remove "Reviewed, then trim
                reviewDate = string.replace(reviewDate, "<span class=\"ratingDate\">Reviewed", "")
                reviewDate = string.replace(reviewDate, "\n</span>", "")
                reviewDate.strip() 
                reviewDate = reviewDate[1:]
                # scan and replace commas with blanks
                reviewDate = string.replace(reviewDate, ",", "")
                dateObj = datetime.strptime(reviewDate, '%B %d %Y')
    except:
        errMsg = "Error fetching review date.  Null value used\n"
        logFile.write(errMsg)
        print errMsg 

    # get helpful review count <span class="numHlp"><span class="numHlpIn">1</span></span>
    try:
        reviewHelpfulCnt = 0
        for tag in soupReviews.find_all('span', class_='numHlpIn'):
            reviewHelpfulCnt = int(tag.contents[0].string)
    except:
        errMsg = 'Error fetching count of review helpful votes.  Null value used.\n'
        logFile.write(errMsg)
        print errMsg 
    
    # end get review data, start get reviewer data
                   
    # get reviewer name and id 
    try:
        reviewerName = ""
        reviewerID = ""
        for tag in soupReviews.find_all('div', class_='username mo'):
            reviewerName = str(tag)
            for tag1 in tag.find_all('span'):
                reviewerName = (unicode(tag1.contents[0].string)) 
                line = str(tag1)
                lines = []
                delim = "mbrName_"
                lines = line.split(delim, 2)
                reviewerID = lines[1][:32]
    except:
        errMsg = "Error fetching reviewer name.  Null value used\n"
        logFile.write(errMsg)
        print errMsg  
      
    # get reviewer location
    try:
        location = ""
        for tag in soupReviews.find_all('div', class_='location'):
            location = (unicode(tag.contents[0].string))
    except:
        errMsg = "Error fetching reviewer location.  Null value used\n"
        logFile.write(errMsg)
        print errMsg    
    
    # get reviewer title  
    try:
        reviewerTitle = ""
        for tag in soupReviews.find_all('div', class_='reviewerTitle'):
            reviewerTitle = str(tag.contents[0].string)
    except:
        errMsg = "Error fetching reviewer title.  Null value used.\n"
        logFile.write(errMsg)
        print errMsg
   
    # get number of reviews reviewer submitted 
    try:
        numReviews = 0
        for tag in soupReviews.find_all('div', class_='totalReviewBadge badge no_cpu'):
            for tag1 in tag.find_all('span', class_='badgeText'): 
                line = tag1.contents[0].string
                delim = ' '
                lines = line.split(delim, 2)
                numReviews = int(lines[0])      
    except:
        errMsg = 'Error fetching number of reviews submitted.  Null value used.\n'
        logFile.write(errMsg)
        print errMsg 

    # get number of hotel reviews submitted
    try:
        numHotelReviews = 0
        for tag in soupReviews.find_all('div', class_='contributionReviewBadge'):
            for tag1 in tag.find_all('span', class_='badgeText'): 
                line = tag1.contents[0].string
                delim = ' '
                lines = line.split(delim, 2)
                numHotelReviews = int(lines[0]) 
    except:
        errMsg = 'Error fetching number of hotel reviews submitted.  Null value used.\n'
        logFile.write(errMsg)
        print errMsg 
 
    # get number of helpful votes
    try:
        helpfulVotes = 0
        for tag in soupReviews.find_all('div', class_='helpfulVotesBadge badge no_cpu'):
            for tag1 in tag.find_all('img'):
                line = tag1['alt']
                delim = ' '
                lines = line.split(delim, 2)
                helpfulVotes = int(lines[0])
    except:
        errMsg = 'Error fetching count of reviewer helpful votes.  Null value used.\n'
        logFile.write(errMsg)
        print errMsg 
        
    try:
        if reviewerIDs.has_key(reviewerID):
            reviewer = None
        else:    
            # reviewerID not found, insert
            reviewer = Reviewer(reviewerName, reviewerID, location, reviewerTitle, numReviews, numHotelReviews, helpfulVotes)
            msg = 'New reviewer found adding reviewer ' + reviewerID + "\n"
            logFile.write(msg)
            print msg 
    except:
        errMsg = "Error checking if review currently in db or creating review.\n"
        logFile.write(errMsg)
        print errMsg  

    try:      
        if (reviewIDs.has_key(reviewID)):
            # review found
            review = None
        else:
            # review not found, create review
            msg = 'New review found adding review ' + reviewID + "\n"
            review = Review(reviewID, "Hotel", hotelName, dateObj, rating, reviewText, reviewerID, prtnrCollcted, reviewHelpfulCnt, starsNum, polarity)
            logFile.write(msg)
            print msg 
    except:
        errMsg = "Error checking if reviewID currently in db or creating reviewer.\n"
        logFile.write(errMsg)
        print errMsg                    
                      
    return 0, runOnce, firstReviewID, review, reviewer 

def GetReviewIDs(conn, cursor):
    # return dict of existing review_ids
 
    reviewIDs = {}
    
    print "Fetching IDs from product_reviews"
    
    sql_cmd = 'select id from product_reviews;'
    cursor.execute(sql_cmd) 
    results = cursor.fetchall()
    for row in results:
        reviewIDs[row[0]] = ''
    
    return reviewIDs

def GetReviewers(conn, cursor):
    # return dict of existing reviewer_ids
 
    reviewerIDs = {}
    
    print "Fetching all IDs from product_reviewers"
    sql_cmd = 'select id from reviewers;'
    cursor.execute(sql_cmd) 
    results = cursor.fetchall()
    for row in results:
        reviewerIDs[row[0]] = ''
  
    return reviewerIDs

def OpenDBConn():
    # declare user, passwd and db
    conn = MySQLdb.connect(host= "localhost", user="", passwd="", db="")
    cursor = conn.cursor()
 
    return conn, cursor    

def InsertReviewsInDB(reviews, conn, cursor, logFile, reviewIDs):
              
    sql_cmd = '''INSERT INTO product_reviews (id, product, name, dt, stars, text, reviewerID, prtnr_collected, helpfulCnt, stars_num, polarity)  
                                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '''
    for r in reviews:    
        try:
            cursor.execute(sql_cmd, (r.id, r.product, r.name, str(r.dt.strftime('%Y-%m-%d')), r.stars, r.text, r.reviewerID, r.prtnrCollected, r.helpfulCnt, r.starsNum, r.polarity)) 
            conn.commit() 
            reviewIDs[r.id] = r.reviewerID
        except:
            conn.rollback()
            msg = 'insert review error.  review id=' + r.id +'\n'
            logFile.write(msg)
            print(msg)
            return -1

    return 0

def InsertReviewersInDB(reviewers, conn, cursor, logFile, reviewerIDs):
              
    sql_cmd = '''INSERT IGNORE INTO reviewers (name, id, location, title, numReviews, numHotelReviews, helpfulVotes) 
                                             VALUES (%s, %s, %s, %s, %s, %s, %s) '''
    for r in reviewers:    
        try:
            cursor.execute(sql_cmd, (r.name, r.id, r.location, r.title, r.numReviews, r.numHotelReviews, r.helpfulVotes)) 
            conn.commit() 
            reviewerIDs[r.id] = ''
        except:
            conn.rollback()
            msg = 'insert reviewer error.  reviewer id=' + r.id +'\n'
            logFile.write(msg)
            print(msg)
            return -1
            
    return 0

def GetHotelName(soup, logFile):
    
    hotelName = ""
    try:
        for tag in soup.find('title'):
            hotelName = ""
            hotelName = str(tag)
            # scan the string to find the last character of the hotel name
            lastChar = string.find(hotelName, ")")
            if (lastChar == -1):
                # character ")" not found returns a -1, restore hotelName string
                hotelName = str(hotelName)
            else:    
                # slice the hotel name
                hotelName = hotelName[:lastChar+1]
            # scan and replace commas with blanks
            hotelName = string.replace(hotelName, ",", "") 
            if (hotelName == ""):
                errMsg = "GetHotelName extract error.  Hotel name not found"
                logFile.write(errMsg)
                print errMsg
                sys.exit()
            return hotelName[:60]  
    except:
        print "Error fetching hotel name.  Null value used"
        return ""

if __name__ == '____':
    onlyNewReviews = True
    ScrapeTripAdvisorHotelPages(onlyNewReviews)
    print "Process compleopted successfully\n" * 3   
