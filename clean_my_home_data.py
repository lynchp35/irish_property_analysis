import pandas as pd 
import numpy as np 
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import pandas_udf

def fix_price(price):
    try:
        return int(price)
    except ValueError:
        return np.nan
        
def fix_floor_area(area):
    ft2m = 10.764
    median_area = 110
    try:
        if len(area.split()) == 2:
            if area.split()[0][-1] == "m":
                return area.split()[0][:-1]
            elif area.split()[0][-2:] == "ft":
                return round((float(area[:-4])/ft2m),2)
            count += 1
        else:
            area1 = float(area.split(" - ")[0])
            area2 = float(area.split(" - ")[1][:-3])
            
            if np.abs(median_area - area1) <= np.abs(median_area - area2):
                return area1
            else:
                return area2
    except AttributeError:
        pass
    
def find_county(address: str, counties: np.ndarray, county_position=-1) -> str:

    """
    The address is comma seperated with the county at the end. (Some adresses are missing the county).
    We use the array counties to check if the county found is actually a county.

    We also need to check for counties such as Dublin 1, Dublin 2 etc.

    """
    try:
        countyName = address.split(",")[county_position].split()
    except IndexError:
        return np.nan
    if len(countyName) == 1:
        if countyName[0].strip().capitalize() in counties or countyName[0].strip().capitalize()[:-1] in counties:
            return countyName[0].capitalize()
        
        elif len(countyName[0].strip().lower().split("co.")) > 1:
            return countyName[0][3:].capitalize()
        
        else:
            return np.nan
        
    elif len(countyName) == 2:
        if countyName[0].strip().capitalize() in counties or countyName[0].strip().capitalize()[:-1] in counties:
            try:
                int(countyName[1].strip())
                return " ".join(countyName)
            
            except ValueError:
                return countyName[0].strip().capitalize()
            
        elif countyName[1].strip().capitalize() in counties or countyName[1].strip().capitalize()[:-1] in counties:
            return countyName[1].strip().capitalize()
        
        else:
            return np.nan
        
    else:
        for value in countyName:
            if value.strip().capitalize() in counties or value.strip().capitalize()[:-1] in counties:
                return value
            
def fix_counties(county):
    try:
        if county[-1] == "." or county[-1] == "-":
            return county[:-1]
        else:
            return county
    except TypeError:
        return np.nan 
    
def find_province(county: str, county_dict: dict) -> str:

    """"
    This function aims to find the province using the county found with the last function.

    It takes the county in and dictionary to return the correct province.
    """

    if county in county_dict: # Works for Counties outside of Dublin
        return county_dict[county]
    elif county.split(" ")[0] == "Dublin": # Works for Dublin
        return county_dict["Dublin"]
    else:
        print(f"County: {county} was not found in dictionary")

def create_county_dict(df: pd.DataFrame) -> dict:

    """
    Takes in dataframe with the columns County and Province,
    then returns a dictionary with the county as the key and the province as the value.
    This dictionary is used in the function above.
    """

    county_dict = {}
    for i in range(len(df)):
        county_dict[df["County"][i]] = df["Province"][i]
    return county_dict


if __name__ == "__main__":

    myhome_listings = pd.read_csv("data/myHome_from_page_1_till_page_790_by_20.csv", index_col=["Unnamed: 0"])
    myhome_listings = myhome_listings.replace({"nan":np.nan}).dropna(subset="Address").reset_index(drop=True)
    
    province_df = pd.read_csv("data/county_data.csv")
    county_dict = create_county_dict(province_df)

    myhome_listings["Price"] = myhome_listings["Price"].apply(lambda x :fix_price(x))
    myhome_listings["floor_area"] = myhome_listings["floor_area"].apply(lambda x :fix_floor_area(x))
    myhome_listings["County"] = myhome_listings["Address"].apply(lambda x :find_county(x, county_dict))
    myhome_listings["County"] = myhome_listings["County"].apply(lambda x: fix_counties(x))

    myhome_listings.dropna(subset=["County"], inplace=True) # Haven't tested this so it maybe wrong.
    myhome_listings["Province"] = myhome_listings["County"].apply(lambda x: find_province(x, county_dict))

    
    myhome_listings.to_csv("data/clean_myhome_listing.csv")
    
    
    sold_df = pd.read_csv("data/myHome_sold_property_from_page_1_till_page_1000.csv", index_col=["Unnamed: 0"])

    sold_df_counties_1 = pd.DataFrame(sold_df["Address"].apply(lambda x : find_county(x, county_dict,county_position=-1)).dropna())
    sold_df_counties_2 = pd.DataFrame(sold_df["Address"].apply(lambda x : find_county(x, county_dict,county_position=-2)).dropna())

    sold_df = sold_df.join(
        sold_df_counties_1.append(
        sold_df_counties_2).sort_index(
        ).rename(columns={"Address":"County"})
        ).drop_duplicates(subset="Address")
    
    
    price_change_df = pd.read_csv("data/myHome_price_change_from_page_1_till_page_349.csv", index_col=["Unnamed: 0"])

    price_change_df_counties_1 = pd.DataFrame(price_change_df["Address"].apply(lambda x : find_county(x, county_dict,county_position=-1)).dropna())
    price_change_df_counties_2 = pd.DataFrame(price_change_df["Address"].apply(lambda x : find_county(x, county_dict,county_position=-2)).dropna())


    price_change_df = price_change_df.join(
        price_change_df_counties_1.append(
        price_change_df_counties_2).sort_index(
        ).rename(columns={"Address":"County"})
        ).drop_duplicates(subset="Address")
    
    sold_df.to_csv("data/clean_myhome_sold.csv")
    price_change_df.to_csv("data/clean_myhome_price_change.csv")
    # I am not sure if there is anything else that we would need to clean.
    # In the next section we could look into imputing some null values and maybe include a hierarchical structure for  Property_type.
    # Currently 11 Property_types but users may want to look at a higher level view.
    # May also look into D3.js for visualisations.


