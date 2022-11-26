import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

class MyHome_price_change_web_scraper:
    
    """ 
    The goal of this class is to scrape property data from the Irish property site Daft.ie.
    The class utilizes PySpark for parralization.
    Creating ....
    
    The parameters for the class are:
    url: This is the base url the model will use. Allows you to search the whole of Ireland or can use a differnt url for a small search.
    start_page: what page the model will start scraping from. Will use for parralization.
    end_page: what page the model will stop scraping at. Will use for parralization.
    page_size: The number of property listings each page should return. Usually set to 20.
    """
    
    def __init__(self, url, start_page, end_page):
        self.url = url
        self.start_page = start_page
        self.end_page = end_page
        self.property_dict = {
                  "Date":[],
                  "Address":[],
                  "New_price":[],
                  "Old_price":[],
                  "Change(€)":[],
                  "Change(%)":[],
                  "Page":[],
                    }
        
    def start_scraping(self):
        """ 
        The function is used to start the scraping process.
        It uses a for loop to go through all the pages in [start_page, end_page],
        and BeautifulSoup() to retreive the page data.
        
        The soup is then passed the extract_data function.
        """
        print("Scarping of MyHome.ie property price change data starting")
        
        counter = self.end_page // 10
        
        for i in range(self.start_page, self.end_page):
            self.i = i
            if self.i % counter == 0:
                progress = (self.i/self.end_page)*100
                print(f"Process is {progress:.2f}% of the way done")
            current_url = f"{self.url}{self.i}"
            response = requests.get(current_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            self.extract_data(soup)
        
        return self.property_dict
    
    def extract_data(self, soup):
        
        """ 
        This function is used to find all property listings using the 'soup' then it checks if the listing is a single listing
        or part of a sublisting.
        A sublisting is common for new builds as the builder is selling multiple properties in the new house developement.
        
        If the listing is a single listing the data is extracted using the extract_single_listing function.
        If it is a sublisting (multiple listings) the data is extracted using the extract_sub_listing function.
        """
        
        for property_listings in soup.find_all("div", "PropertyPriceChangeCard__Info MhHelper__Flex--spaced"):

            address = property_listings.find_all("a", "PropertyPriceChangeCard__Address")[0].text
            date = pd.to_datetime(property_listings.find_all("span", "PriceRegisterListItem__Date")[0].text.strip())

            price_data = property_listings.find_all("div", "PropertyPriceChangeCard__PriceChangeItem")
            p1 = price_data[0]
            p2 = price_data[1]

            change_euro = self.convert_price_to_int(p1.find_all("span")[0].text)
            change_precentage = float(p1.find_all("span")[1].text[1:-2])
            if change_precentage < 0:
                change_euro = change_euro *-1
            old_price = self.convert_price_to_int(p2.find_all("span")[0].text)
            new_price = self.convert_price_to_int(p2.find_all("span")[1].text)

            self.property_dict["Date"].append(date)
            self.property_dict["Address"].append(address)
            self.property_dict["New_price"].append(new_price)
            self.property_dict["Old_price"].append(old_price)
            self.property_dict["Change(€)"].append(change_euro)
            self.property_dict["Change(%)"].append(change_precentage)
            self.property_dict["Page"].append(self.i)

            # add null if any values are missing
            self.imput_null_values()
            
            
    def imput_null_values(self):

        """
        Appends a null value for features in the dict which are missing an entry. 
        """
        maxLen = max([len(self.property_dict[key]) for key in self.property_dict])
        for key in self.property_dict:
            if len(self.property_dict[key]) < maxLen:
                self.property_dict[key].append(np.nan)

        if sum(np.array([len(self.property_dict[key]) for key in self.property_dict]) == maxLen) != len(self.property_dict):
            print("Error dictionary not of uniform length")
            self.imput_null_values()
    
    def convert_price_to_int(self, price):
        
        """
        This function tries to convert the price to an int if not returns the orginal string.
        """
        try:
            price = int("".join(price.split("€")[1].split(",")))
        except:
            if price.strip() in ["Price on Application", "POA", "AMV: Price on Application"]:
                price = "PoA"
            else:
                print(price, self.i)
                price = price.strip()
        return price

if __name__ == "__main__":
    path = "../data/"

    url = "https://www.myhome.ie/pricechanges/page-"
    start_page = 1
    end_page = 350

    s1 = MyHome_price_change_web_scraper(url, start_page, end_page)

    property_dict = s1.start_scraping()

    properties = pd.DataFrame(property_dict)
    
    properties.to_csv(f"{path}myHome_price_change_from_page_{start_page}_till_page_{end_page-1}.csv")
