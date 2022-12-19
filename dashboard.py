# Import libraries
import pandas as pd
import numpy as np
import math

from pyspark.sql.functions import col, udf, when

import geopandas
import json

from ipywidgets import interact

from bokeh.io import output_notebook, show, output_file, push_notebook, save
from bokeh.plotting import figure

from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar, NumeralTickFormatter, ColumnDataSource, HoverTool, Div
from bokeh.palettes import brewer
from bokeh.transform import factor_cmap, jitter

from bokeh.io.doc import curdoc
from bokeh.models import Slider, HoverTool, Select, CustomJS
from bokeh.layouts import row, column, gridplot

# Import SparkSession
from pyspark.sql import SparkSession

def create_barchart(SparkDF, groupby_columns=['Province', 'County'], value="Price_mean"):
    # Plot a complex chart with interactive hover in a few lines of code
    
    SparkDF_simp = SparkDF.withColumn('County', when(SparkDF.County.startswith('Dublin'), "Dublin").otherwise(SparkDF.County))
    
    df = SparkDF_simp.toPandas()

    group = df.groupby(by=groupby_columns)
    source = ColumnDataSource(group)

    p = figure(width=800, height=400, title=f"Property {value} by {groupby_columns[0]} and {groupby_columns[1]}",
               x_range=group, toolbar_location=None, tools="")

    p.xgrid.grid_line_color = None
    p.xaxis.axis_label = f"{groupby_columns[1]} grouped by {groupby_columns[0]}"
    p.xaxis.major_label_orientation = 1.2
    p.xaxis.major_label_orientation = math.pi/2

    index_cmap = factor_cmap(f'{groupby_columns[0]}_{groupby_columns[1]}', palette=['#2b83ba', '#abdda4', '#ffffbf', '#fdae61', '#d7191c'], 
                             factors=sorted(df.Province.unique()), end=1)

    p.vbar(x=f'{groupby_columns[0]}_{groupby_columns[1]}', top=f"{value}", width=1, source=source,
           line_color="white", fill_color=index_cmap, 
           hover_line_color="darkgrey", hover_fill_color=index_cmap)

    p.add_tools(HoverTool(tooltips=[(f"{value}", f"@{value}"), 
            (f"{groupby_columns[0]}, {groupby_columns[1]}", f'@{groupby_columns[0]}_{groupby_columns[1]}')]))

    return p

def scatter_plots_price(SparkDF, colName):
    
    df = SparkDF.toPandas()
    df = df[df["Floor_area"] < 2000]
    source = ColumnDataSource(df)
    
    
    if colName != "Floor_area":
        x_range = np.array(sorted(df[colName].unique())).astype(str)
        p = figure(width=800, height=300, x_range=x_range,
                   title=f"Scatterplot of price and {colName}")
        p.circle(y='Price', x=jitter(f'{colName}', width=0.6, range=p.x_range),  source=source, alpha=0.3)
    
    else:
        p = figure(width=800, height=300,
                   title=f"Scatterplot of price and {colName}")
        p.circle(y='Price', x=jitter(f'{colName}', width=0.6),  source=source, alpha=0.3)

    p.x_range.range_padding = 0
    p.ygrid.grid_line_color = None

    return p


if __name__ == "__main__":
    
    # Create SparkSession 
    spark = SparkSession.builder \
          .master("local[1]") \
          .appName("SparkByExamples.com") \
          .getOrCreate()
    
    df = pd.read_csv("data/cleaned/all_listings.csv", index_col=["Unnamed: 0"]).dropna()
    df=spark.createDataFrame(df)
    
    output_file("static_dashboard.html")

    complete_price_mean = create_barchart(df, value="Price_mean")
    complete_price_count = create_barchart(df, value="Price_count")
    
    intro = Div(
        text="""
              <h1>Irish Property Dashboard</h1>
              <p>The data used in the dashboards was scraped from Daft.ie and myhome.ie.</p>
              <p>The dashboard can be broken into two parts.</p>
              <ol>
              <li>Static Nationwide Graphs</li>
              <li>Interactive Graphs</li>
              </ol>
              """,

    )
    static_dashboards = Div(
        text="""
              <h3>Static Nationwide Graphs</h3>
              <p>The first two graphs illustarte the 
              differences in property price and property 
              sales between counties and provinces in Ireland.
              <br>As you can see Dublin on average is the most expensive,
              but also has the most sales.
              <br>The next three graphs are scatterplots of the features Beds, 
              Baths and Floor area againist the property price. 
              <br>Typically as the number of bedrooms, bathrooms and floor area increases so does the price.</p>
              """,

    )

    interactive_dashboards = Div(
        text="""
              <h3>Interactive Graphs</h3>
              <p> Enter some data and learn about the property market</p>
              """,

    )

    complete_price_mean = create_barchart(df, value="Price_mean")
    complete_price_count = create_barchart(df, value="Price_count")

    sp_beds = scatter_plots_price(df, "Beds")
    sp_baths = scatter_plots_price(df, "Baths")
    sp_floor_area = scatter_plots_price(df, "Floor_area")


    # show the results in a row
    save(gridplot([[intro], [static_dashboards], 
    [complete_price_mean, complete_price_count], 
    [sp_beds, sp_baths],[sp_floor_area],[interactive_dashboards]]))