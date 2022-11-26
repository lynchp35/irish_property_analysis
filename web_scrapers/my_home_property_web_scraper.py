import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

class MyHome_web_scraper:
    
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
        self.property_dict = {"beds":[],
                  "baths":[],
                  "floor_area":[],
                  "Price":[],
                  "Address":[],
                  "Property_type":[],
                  "New_build":[],
                  "Page":[],
                    }
        
    def start_scraping(self):
        """ 
        The function is used to start the scraping process.
        It uses a for loop to go through all the pages in [start_page, end_page],
        and BeautifulSoup() to retreive the page data.
        
        The soup is then passed the extract_data function.
        """
        print("Scarping of MyHome.ie starting")
        
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
        
        for property_listings in soup.find_all("div", "mb-3 ng-star-inserted"):
            
            # Details include beds, bath, m2 and property_type
            property_details = property_listings.find_all("span", "PropertyInfoStrip__Detail PropertyInfoStrip__Detail--dark ng-star-inserted")
            self.extract_property_details(property_details)
            
            #Price and ddress of the property
            price = property_listings.find_all("div", re.compile("^ng-tns.*ng-star-inserted$"))
            price = self.convert_price_to_int(price[0].text)
            address = property_listings.find_all("a",re.compile(".*PropertyListingCard"))
            
            #add values to dict
            self.property_dict["Price"].append(price)
            self.property_dict["Address"].append(address[0].text)
            self.property_dict["Page"].append(self.i)  
            self.property_dict["New_build"].append(np.nan)
            
            # add null if any values are missing
            self.imput_null_values()
            
    def extract_property_details(self, property_details):

        """
        This function extracts the number of beds, baths and floor-area for single listed properties.
        """

        for detail in property_details:
            # This will be either bed, bath or property_type
            if len(detail) == 2:
                detail_list = detail.text.strip().split(" ")

                if len(detail_list) == 2:
                    if detail_list[1] in ["beds", "baths"]:
                        self.property_dict[detail_list[1]].append(detail_list[0])
                    elif detail_list[1] in ["bed", "bath"]:
                        self.property_dict[f"{detail_list[1]}s"].append(detail_list[0])
                    else:
                        self.property_dict["Property_type"].append(" ".join(detail_list))

                elif len(detail_list) == 1 or len(detail_list) == 4:
                    self.property_dict["Property_type"].append(detail_list[0])

                else:
                    print(f"Property detail didn't fit into bed, bath or property_type: {detail_list}")

            # This will be floor_area
            elif len(detail) == 3:
                floor_area = detail.text
                if floor_area[-3:] == "m 2" or floor_area[-4:] == "ft 2":
                    self.property_dict["floor_area"].append(floor_area)
                else:
                    print(f"floor area not in ft or m but in type {floor_area}")

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

    url = "https://www.myhome.ie/residential/ireland/property-for-sale?page="
    start_page = 1
    end_page = 791

    s1 = MyHome_web_scraper(url, start_page, end_page)

    property_dict = s1.start_scraping()

    properties = pd.DataFrame(property_dict)
    
    properties.to_csv(f"{path}myHome_from_page_{start_page}_till_page_{end_page-1}_by_20.csv")
