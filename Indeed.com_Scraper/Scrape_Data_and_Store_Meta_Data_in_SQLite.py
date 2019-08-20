## This Python file contains the same code as the .ipynb file found in this folder

###########################################################
# ### Scraping HTML pages from indeed.com 
'''
This program takes a list of job titles and a list of cities, it performs a job search on indeed.com, and it saves the HTML page of each job posting found. All the job posting .html files are saved in one folder and  the meta data associated with each posting is saved in an SQL database stored in a .sqlite file.
'''
# ##### Steps that the program is performing:
# 1. Loop through all the job titles in the list.
# 2. For every job title, loop through all the cities in the list.
# 3. For each job and city, perform a search on indeed.com.
# 4. Go though all the results pages, extract the ID of each posting and perform the following 2 tasks:
#     - Store the meta data about the posting in the job_search SQL table. The "job_search" table stores the following information:
#           id              : auto-increment ID
#           job_seach_word  : the word used to perform the search on indeed.com
#           city            : the city name used to perform the search on indeed.com
#           province        : the province name used to perform the search on indeed.com
#           page            : the number of the results page that the job posting was found on
#           jk_id           : the job ID associated with each posting given by indeed.com
#           url             : the URL of the individual job posting.
#
#     - Check to see if the jk_id for each listing is present in the listings SQL table. If it is not, store the jk_id and the url of the posting in the job_urls list. 
# 5. Go though all the jk_ids and the urls in job_url list. These job listings have not been yet scraped. For each job posting perform the following task:
#     - Access the URL of the job posting.
#     - Save the HTML code in a .html file locally on the computer. 
#     - Get the jk_id and url for up to 5 recommended jobs showed on this listing. Add a new row to the "listing table" that includes the following information.
#           id                  : auto-increment ID
#           jk_id               : the job_id of this specific posting found on indeed.com
#           title               : the title of the job posting
#           recommendation 1_jk : the jk_id of the first recommended job found on this listing.
#           recommendation 2_jk : the jk_id of the second recommended job found on this listing.
#           recommendation 3_jk : the jk_id of the third recommended job found on this listing.
#           recommendation 4_jk : the jk_id of the forth recommended job found on this listing.
#           recommendation 5_jk : the jk_id of the fifth recommended job found on this listing.
#
#     - For each recommended job found of the job posting, perform the following two tasks:
#         - Add a new line to the "recommendation table" that includes the following information:
#           id          : auto-increment ID
#           jk_id       : the jk_id of the recommendation posting
#           url         : the url of the recommentation posting
#           parent_jk   : the jk_id of the job listing where the recommended job was found
#
#         - Check to see if the recommended job is in the listing table. If it is not, make a request and access the url of the recommended job posting, save the HTML code in a .html file on the computer and add a new line to the listings table. 
# 

# Assumptions:
#     - every job has a unique jk_id it found in the href of <a> tag on the webpage. 
#     - the jk_id is found in the URL of the job posting
#     - for every job posting, up to 5 recommended jobs will scraped 


import requests
from bs4 import BeautifulSoup as bs
import random
import time
import logging
import datetime as dt
import pandas as pd
import os

from sqlalchemy import create_engine, inspect
from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session


# ## Set up database in sqlite file

path = "indeed_scraped_db"
engine = create_engine(f"sqlite:///{path}")
conn = engine.connect()
session = Session(bind=engine)

Base = declarative_base()

class Job_Search(Base):
    __tablename__ = "job_seach"
    id = Column(Integer, primary_key = True)
    job_search_word = Column(String(255))
    city = Column(String(255))
    province = Column(String(255))
    page = Column(Integer)
    jk_id = Column(String(255))
    url = Column(String(255)) 

class Listing(Base):
    __tablename__ = "listing"
    id = Column(Integer, primary_key = True)
    jk_id = Column(String(255))
    title = Column(String(255))
    recommendation1_jk = Column(String(255))
    recommendation2_jk = Column(String(255))
    recommendation3_jk = Column(String(255))
    recommendation4_jk = Column(String(255))
    recommendation5_jk = Column(String(255))

class Recommendation(Base):
    __tablename__ = "recommendation"
    id = Column(Integer, primary_key = True)
    jk_id = Column(String(255))
    rec_url = Column(String(255))
    parent_jk = Column(String(255))

Base.metadata.create_all(engine) 


today = dt.datetime.now().strftime("%d-%b-%Y")
f = f"Logging {today}.txt"
logging.basicConfig(filename = f,level = logging.DEBUG)


# ###### Funcation: get_jobURL_and_jk(href)
# Input: string containing the href value found in the < a >  tag that contains the link to a single jobpost.
# - The jk_id is found in the href. 
# - An URL can be built from the href.
# 
# Output: Returns the 16-digit jk_id and the URL of a single job_posting

def get_jobURL_and_jk(href):
    jk_index = href.find("?jk=")
    if jk_index > 0:
        view_job = "https://ca.indeed.com/viewjob"
        #'/rc/clk?jk=3a3311124a5e8634&fccid=14a67846d05c8406&vjs=3'
        jk_index = href.find("?jk=")
        end_index = href[jk_index:].find("&")
        jk_id = href[jk_index + 4:jk_index + end_index]
        job_link = view_job + "?jk=" + jk_id   
    elif '/company/' in href:
        ca_indeed = "https://ca.indeed.com" 
        #'/company/Yappn-Canada-Inc/jobs/Machine-Learning-Engineer-489466b291064d2a?fccid=7a6bbd753931f130&vjs=3'
        start = href.find('/jobs/')
        end = href[start:].find('?')
        jk_id = href[start:start+end][-16:]
        job_link = ca_indeed + href
    else:
        return None,None
    return job_link,jk_id


# #### Function: get_job_search_results(job_name,city_name,province_name)
# Input :
# - job_name : string containing the job_name used for the search on indeed.com
# - city_name : string containing name of the city for the search. Spaces are allowed.
# - province_name : string containing name of the province for the search. Spaces are allowed.
# 
# Steps included in this function:
# - make a request to indeed.com to seaching for a job_name in a particular location
# - for every listing on every result page, record the jk_id, url and page number where the listing was found. Store this info in the SQL Job_Search table
# - create a list of jk_ids and URLs for all the listings in the result page that are not present in the SQL Listing table
# 
# Output:
# - job_urls : a list of [jk_id , URL] of the listings that were found on the results page and not yet present in the SQL Listing table 

def get_job_search_results(job_name, city_name, province_name):
    #Crete initial link to start a search based on job_name, city_name and province_name
    job = job_name.replace(" ","+")
    city = city_name.replace(" ","+")
    province = province_name.replace(" ","+")
    starter_link = "https://ca.indeed.com/jobs?q=" + job + "&l=" + city + "%2C+" + province + "&start=0"
    
    #Keep a list that stores all the job URLs that need to be accessed
    #These jobs are not in the listing table
    job_urls = []
    
    #Get the job links
    keep_going = True #determined by the presence of Next button on the page
    page_num = 1
    page_link = starter_link
    while keep_going:
        # Get the html code on the first page that contains multiple job postings
        try:
            r = requests.get(page_link)
        except Exception as e:
            logging.error(f"Requests ERROR: {page_link} could not access the result search page {page_num}.")
            logging.error(e)
            continue
        if r.status_code == 200:
            page_soup = bs(r.text,"html.parser")
            # Get all the listings on the page
            listings = page_soup.find_all('div', class_ = "jobsearch-SerpJobCard")
            # For each listing on the result search page
            for listing in listings:
                href = listing.a["href"]
                job_link,jk_id = get_jobURL_and_jk(href)
                if job_link == None or jk_id == None:
                    logging.info(f"Job_link and jk_id NOT FOUND in href {href}")
                    continue
                #Check to see if the job is already in listings
                if session.query(Listing).filter(Listing.jk_id == jk_id).count() == 0:
                    job_urls.append([jk_id,job_link])
                #Add listing meta data to Job_Search
                job_search = Job_Search(job_search_word = job_name,
                                       city = city_name,
                                       province = province_name,
                                       page = page_num,
                                       jk_id = jk_id,
                                       url = job_link)
                session.add(job_search)
            session.commit()

            #Check to see if there is a next page
            prev_next_buttons = page_soup.find_all('span','np')
            if len(prev_next_buttons) == 0:
                logging.info(f"No Next or Previous buttons were found on page {page_num} when searching {job_name}                 in {city_name},{province_name}. No span np tag were found.")
                keep_going = False
            else:
                if "Next" in prev_next_buttons[0].text or "Next" in prev_next_buttons[-1].text:
                    page_num += 1
                    eq_index = -page_link[::-1].find("=")
                    page_link = page_link[:eq_index] + str(int(page_link[eq_index:]) + 10)
                else:
                    keep_going = False
                time.sleep(random.randint(1,10))
        #keep_going = False #####
    return job_urls


# #### Function: get_job_listings(job_urls)
# Input :
# - job_urls : a list of lists. Each item in the list is a list with two items: [jk_id, URL]. These are the jk_id and the URL of listings that are not currently present in the listings table.
# 
# Steps included in this function:
# - Create a folder if it does not exist to store the .html files.
# - For each list in job_urls do the following tasks:
#     - Access the job listing URL. Get the jk_id of all the recommended jobs on the page. For each recommended job:
#         - Add a new row to the recommendation table
#         - Check to see if the recommended job is present in listings table. If it is not, make a request to the recommendation URL, save the HTML of the recommendation listing to file, find the recommendations present on this recommendation listing and add a new row to the listings table. 
#     - Save the HTML code of the posting to a .html file. 
# 
# Output:
# - The function does not return an output. However, it creates a Job_Postings_Raw_HTML folder locally if it does not exist and adds to it .html files that contain individual job postings.

# Access all the new jobs found on the results pages and download the html page of the posting. 
# All the listings in job_urls are not present in the Listings table
def get_job_listings(job_urls):
    #Create file for html job posts if it does not exist 
    folder = "Job_Postings_Raw_HTML"
    job_path = os.path.join(folder)
    if not os.path.exists(job_path):
        os.makedirs(job_path)  
    
    # Make a url request, get the html code and store it in an html file
    # Add info to listings table
    for jk_url in job_urls:
        jk_id = jk_url[0]
        url = jk_url[1]
        try:
            r = requests.get(url)
        except Exception as e:
            logging.error(f"Requests ERROR: {jk_url} could not be accessed.")
            logging.error(e)
            continue
        soup = bs(r.text, 'html.parser')
        title = soup.title.string
        
        #Find the recommendations and store them in the file
        recs = soup.find_all('div',class_ = "icl-JobResult")
        rec_jk = [] #List that will store 5 jk IDs 
        rec_url = [] # List that will store 5 URLs
        for i in range(len(recs)):
            if recs[i].find("a") != None:
                href_rec = recs[i].a["href"]
                job_link, job_jk = get_jobURL_and_jk(href_rec)
                rec_jk.append(job_jk)
                rec_url.append(job_link)
                recommendation = Recommendation(jk_id = job_jk, rec_url = job_link, parent_jk = jk_id)
                session.add(recommendation)
        session.commit()
        
        #Check to see if the recommended listings were already scraped, if not, scrape them.
        
        for j in range(len(rec_jk)):
            #if session.query(Listing.jk_id).filter_by(jk_id = rec_jk[j]).scalar() is None:
            if session.query(Listing.jk_id).filter_by(jk_id = rec_jk[j]).first() is None:
                #The recommeded posting was not scraped before
                try:
                    rec_request = requests.get(rec_url[j]).text
                except Exception as e:
                    logging.error(f"Recommendation URL {rec_url[j]} could not be accessed")
                    logging.error(e)
                    continue
                rec_soup = bs(rec_request, 'html.parser')
                rec_title = rec_soup.title.string
                #Write HTML code to file 
                file_name_recommendation = ''.join(e for e in title if e.isalnum() or e == " ") + "_" + rec_jk[j]
                current_path_rec = os.path.join(folder,file_name_recommendation + " RAW.html")
                with open(current_path_rec,'w') as f:
                    f.write(str(rec_soup))
                #Find the jk_id of the recommended jobs on this page.
                recs_on_rec = rec_soup.find_all('div',class_ = "icl-JobResult")
                recs_on_rec_jk = []
                for rec_on_rec in recs_on_rec:
                    if rec_on_rec.find("a") != None:
                        href_rec_on_rec = rec_on_rec.a["href"]
                        rec_on_rec_link, rec_on_rec_jk = get_jobURL_and_jk(href_rec_on_rec)
                        recs_on_rec_jk.append(rec_on_rec_jk)
                for i in range(5-len(recs_on_rec_jk)):
                    recs_on_rec_jk.append(None)
                recommendation_listing = Listing(jk_id = rec_jk[j], title = rec_title, 
                                                 recommendation1_jk = recs_on_rec_jk[0],recommendation2_jk = recs_on_rec_jk[1],
                                                recommendation3_jk = recs_on_rec_jk[2],recommendation4_jk = recs_on_rec_jk[3],
                                                recommendation5_jk = recs_on_rec_jk[4])
                session.add(recommendation_listing)
                #print("Added recommention " + str(rec_jk[j]))
                session.commit()
        
        #If the listing has less than 5 recommendations, add None to the list to make it 5.
        for i in range(5 - len(rec_jk)):
            rec_jk.append(None)
        
        
        listing = Listing(jk_id = jk_id, title = title, recommendation1_jk = rec_jk[0],
                         recommendation2_jk = rec_jk[1], recommendation3_jk = rec_jk[2],
                         recommendation4_jk = rec_jk[3], recommendation5_jk = rec_jk[4])
        session.add(listing)
        session.commit()
        
        file_name = ''.join(e for e in title if e.isalnum() or e == " ") + "_" + jk_id
        current_path = os.path.join(folder,file_name + " RAW.html")
        with open(current_path,'w') as f:
            f.write(str(soup))
        time.sleep(random.randint(1,5))

###########################################################
# ## Program Starts Here
###########################################################


jobs = ["Machine Learning"]
cities = ["Toronto,ON"]
for job in jobs:
    for city_prov in cities:
        city, province = city_prov.split(",") 
        urls = get_job_search_results(job,city,province)
        get_job_listings(urls)


# See rows in the Job_Search table 


job_search = pd.read_sql(session.query(Job_Search).statement,session.bind)
#job_search.head()


# See rows in the Listings table

listing_df = pd.read_sql(session.query(Listing).statement,session.bind) 
#listing_df.head()


# See rows in the Recommendation table

recommendation = pd.read_sql(session.query(Recommendation).statement,session.bind) 
#recommendation.head()
