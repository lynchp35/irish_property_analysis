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
        self.property_dict = {"Date":[],
              "Address":[],
              "Price":[],
              "Page":[],
                    }
        
    def start_scraping(self):
        """ 
        The function is used to start the scraping process.
        It uses a for loop to go through all the pages in [start_page, end_page],
        and BeautifulSoup() to retreive the page data.
        
        The soup is then passed the extract_data function.
        """
        print("Scarping of MyHome.ie sold property data starting")
        
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
        
        for property_listings in soup.find_all("div", "PriceRegisterListItem SoldPropertyListItem"):
        
            address = property_listings.find_all("a", "SoldPropertyListItem__Address")[0].text.strip()
            date = pd.to_datetime(property_listings.find_all("span", "PriceRegisterListItem__Date")[0].text.strip())

            price = property_listings.find_all("span", "ng-star-inserted")[1].text
            price = self.convert_price_to_int(price)

            self.property_dict["Date"].append(date)
            self.property_dict["Address"].append(address)
            self.property_dict["Price"].append(price)
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

    url = "https://www.myhome.ie/priceregister/page-"
    start_page = 1
    end_page = 1001

    s1 = MyHome_price_change_web_scraper(url, start_page, end_page)

    property_dict = s1.start_scraping()

    properties = pd.DataFrame(property_dict)
    
    properties.to_csv(f"{path}myHome_sold_property_from_page_{start_page}_till_page_{end_page-1}.csv")
