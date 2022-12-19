import pandas as pd
from bokeh.io import curdoc, output_file, save
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Div, Select, Slider, TextInput, AutocompleteInput
from bokeh.plotting import figure
from os.path import dirname, join
import pandas.io.sql as psql
import sqlite3 as sql
from sqlalchemy import create_engine
import time
engine = create_engine('sqlite://', echo=False)

output_file("interactive_dashboard.html")

desc = Div(text=open("description.html").read(), sizing_mode="stretch_width")
pd_df = pd.read_csv("data/cleaned/all_listings.csv", index_col=["Unnamed: 0"]).dropna()

completion_list = list(pd_df["County"].unique())
sql_df = psql.to_sql(pd_df, 'property', con=engine, if_exists="replace")

axis_map = {
    "Number of beds": "Beds",
    "Number of baths": "Baths",
    "Size of the property": "Floor_area",
    "Price": "Price",
}

# Create Input controls
min_beds = Slider(title="Minimum number of beds", value=0, start=0, end=20, step=1)
max_beds = Slider(title="Maximum number of beds", value=1, start=1, end=20, step=1)

min_baths = Slider(title="Minimum number of baths", value=0, start=0, end=20, step=1)
max_baths = Slider(title="Maximum number of baths", value=1, start=1, end=20, step=1)

min_price = Slider(title="Minimum price", value=0, start=0, end=10_000_000, step=20000)
max_price = Slider(title="Maximum price", value=100_000, start=100_000, end=10_000_000, step=20000)

min_floor_area = Slider(title="Minimum floor area", value=0, start=0, end=3000, step=100)
max_floor_area = Slider(title="Maximum floor area", value=200, start=200, end=3000, step=100)



counties_slider =  AutocompleteInput(title="Enter a County:", value="All", completions=completion_list)


LABELS = ["House", "Apartment", "Site"]
property_slider =  AutocompleteInput(title="Pick a property category:", value="All", completions=LABELS)


x_axis = Select(title="X Axis", options=sorted(axis_map.keys()), value="Size of the property")
y_axis = Select(title="Y Axis", options=sorted(axis_map.keys()), value="Price")

# Create Column Data Source that will be used by the plot
source = ColumnDataSource(data=dict(x=[], y=[], Price=[], Property_type=[], County=[]))

TOOLTIPS=[
    ("$", "@Price"),
    ("Property_type", "@Property_type"),
    ("County", "@County"),
]

p = figure(height=600, title="", toolbar_location=None, tooltips=TOOLTIPS, sizing_mode="stretch_width")
p.circle(x="x", y="y", source=source, size=7, line_color=None,)


def select_properties():
    
    selected = pd.read_sql_query(
    f'SELECT * FROM property WHERE Beds >= {min_beds.value} AND Beds <= {max_beds.value} AND Baths >= {min_baths.value} AND Baths <= {max_baths.value} AND Price >= {min_price.value} AND Price <= {max_price.value} AND Floor_area >= {min_floor_area.value} AND Floor_area <= {max_floor_area.value}', 
    engine)
    if (counties_slider.value != "All"):
        selected = selected[selected["County"] == counties_slider.value]
    if (property_slider.value != "All"):
        selected = selected[selected["Property_Category"] == property_slider.value]
    return selected.drop(columns=["index"])


def update():
    df = select_properties()
    x_name = axis_map[x_axis.value]
    y_name = axis_map[y_axis.value]
    
    p.xaxis.axis_label = x_axis.value
    p.yaxis.axis_label = y_axis.value
    p.title.text = f"{len(df)} properties selected"
    source.data = dict(
        x=df[x_name],
        y=df[y_name],
        Price=df["Price"],
        Property_type=df["Property_type"],
        County=df["County"],
    )


controls = [min_beds, max_beds, min_baths, max_baths, 
            min_price, max_price, min_floor_area, 
            max_floor_area,counties_slider, property_slider,
            x_axis, y_axis]


inputs = column(*controls, width=320, height=800)

layout = column(row(inputs, p, sizing_mode="inherit"), sizing_mode="stretch_width", height=800)
for control in controls:
    control.on_change('value', lambda attr, old, new: update())
    update()  # initial load of the data

curdoc().add_root(layout)
curdoc().title = "Properties"
