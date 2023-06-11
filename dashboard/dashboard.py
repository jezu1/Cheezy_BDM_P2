# -*- coding: utf-8 -*-
"""
Creating a streamlit application for restaurant dashboards
"""
import streamlit as st
import base64
import plotly.express as px
import pandas as pd
import math

import pyspark
from delta import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, date_format, avg, countDistinct

from pyspark.ml.image import ImageSchema
from pyspark.ml.linalg import DenseVector, VectorUDT
import numpy as np
from PIL import Image
import pyspark.sql.functions as F

# cd C:\Users\Adina Bondoc\Documents\Python\Masters\UPC\BDM
image_path="hdfs://localhost:9000/user/hadoop/images"

rid=3
path_dw="/user/hadoop/delta/warehouse"
path_save="/user/hadoop/delta/datamart"

# Fix layout
st.set_page_config(layout="wide", page_title="Cheezy Dashboard")

#=============================================================================#
#                                PYSPARK SETUP                                #
#=============================================================================#

# Set up spark session
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def read_spark_df(tablename, path):
    """
    Read file as spark dataframe given a tablename and directory
    """
    complete_file=f"{path}/{tablename}"
    return spark.read.format("delta").load(f"hdfs://localhost:9000{complete_file}")

@st.experimental_memo()
def load_data(rid, 
              path_save="/user/hadoop/delta/datamart",
              path_dw="/user/hadoop/delta/warehouse"):
    """
    Load data for streamlit app. 
    Input:
        rid: int Restaurant id
        path_save: path for datamart
        path_dw: path for data warehouse
    """
    
    # Read data and filter for rid
    monthly_resto_users=read_spark_df('monthly_resto_users', path_save).filter(col('restaurantId')==rid).toPandas()
    monthly_resto_images=read_spark_df('monthly_resto_images', path_save).filter(col('restaurantId')==rid).toPandas()
    daily_resto=read_spark_df('daily_resto', path_save).filter(col('restaurantId')==rid).withColumn("month", date_format('date','yyyy-MM')).toPandas()
    
    cuisines=(read_spark_df('cuisines', path_dw)
              .withColumnRenamed('restaurant_key','restaurantId')
#               .filter(col('restaurantId')==rid)
             )
    monthly_resto=read_spark_df('monthly_resto', path_save)
    
    clist=cuisines.filter(col('restaurantId')==rid).select('cuisine').distinct().collect()
    clist=[v['cuisine'] for v in clist]

    # Get ranking of each restaurant per cuisine
    window1 = Window.partitionBy("cuisine",'month').orderBy(col("dly_avg_impressions").desc())
    cuisine_ranking=(monthly_resto
                    .drop('restaurant_name')
                    .join(cuisines.where(F.col("cuisine").isin(clist)), 
                          on=['restaurantId'], how='right')
                    .withColumn("crank",row_number().over(window1))
                    .sort(*["cuisine",'month','crank'])
                   )

    # Get average stats per cuisine and month
    per_cuisine=cuisine_ranking.groupBy(['cuisine','month']).agg(
                    avg('dly_avg_users').alias('dly_avg_users'),
                    avg('dly_avg_impressions').alias('dly_avg_impressions'),
                    avg('dly_avg_left_swipes').alias('dly_avg_left_swipes'),
                    avg('days_active').alias('days_active'),
                    countDistinct('restaurantId').alias('restos')
                ).toPandas()

    # Get per cuisine restaurant rank monthly
    cuisine_ranking=(cuisine_ranking
                     .filter(col('restaurantId')==rid)
                     .select(['month','cuisine','rank','crank'])
                     .toPandas())
    
    # Get available months
    months=monthly_resto.filter(col('restaurantId')==rid).select('month').distinct().collect()
    months=[i[0] for i in months]
    
    # Monthly statistics of a specific restaurant
    monthly_resto=monthly_resto.filter(col('restaurantId')==rid).toPandas()
    
    return (sorted(months), 
            monthly_resto_users, 
            monthly_resto_images, 
            daily_resto, 
            per_cuisine, cuisine_ranking, 
            monthly_resto)

@st.experimental_memo()
def filter_month(month):
    """
    Filter dataframes by month. Automatically creates filtered global variables
    """
    dfs=['monthly_resto_users', 
                'monthly_resto_images', 
                'daily_resto', 
                'per_cuisine', 'cuisine_ranking', 
                'monthly_resto']
    r=[]
    for df in dfs:
        cols=globals()[df].columns
        drop_cols=[i for i in ['month','restaurantId'] if i in cols]
        r.append(globals()[df][globals()[df]['month']==month].drop(columns=drop_cols))
    
    return r

def spark_read_image(img_file):
    """
    Read image from hdfs. returns a PIL image
    based on: https://stackoverflow.com/questions/71823371/convert-an-image-in-a-pyspark-dataframe-to-a-numpy-array
    """
    df=spark.read.format("image").load(img_file)

    img2vec = F.udf(lambda x: DenseVector(ImageSchema.toNDArray(x).flatten()), VectorUDT())
    df_new = df.withColumn('vecs',img2vec('image'))

    row_dict = df_new.first().asDict()
    img_vec = row_dict['vecs']

    img_dict = row_dict['image']
    width = img_dict['width']
    height = img_dict['height']
    nChannels = img_dict['nChannels']
    img_np = img_vec.reshape(height, width, nChannels)

    m = np.ma.masked_greater(img_np, 100)
    m_mask = m.mask
    args = np.argwhere(m_mask)

    return Image.fromarray(np.uint8(img_np)).convert('RGB')

#=============================================================================#
#                              Helper functions                               #
#=============================================================================#

millnames = ['',' K',' M',' B',' T']

def millify(n):
    """
    Convert numeric to human readable format
    source:https://stackoverflow.com/questions/3154460/python-human-readable-large-numbers
    """
    if not math.isnan(n):
        n = float(n)
        millidx = max(0,min(len(millnames)-1,
                            int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))
    
        return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])
    else:
        return ""

def prettify_fig(fig, xtitle=None, ytitle=None, showlegend=False):
    """
    Apply generic plotly template to given graph
    """
    fig.update_layout(template='plotly_white',
                      xaxis={'title':xtitle},
                    yaxis={'title':ytitle},
                    showlegend=showlegend,
                    margin=dict(t=20, b=50),
                       paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)'
                      )

def add_avehline(fig, df, metric, col="#ffab40"):
    """
    Add a horizontal line to an existing plotly figure
    input:
        df = dataframe
        metric = metric of df you want to get
        col = color
    """
    y=df[metric].mean()
    fig.add_hline(y=y, line_width=3, line_dash="dot", line_color=col)
    fig.add_annotation(x=1.05, y=y,
                       xref="paper",
                       text=f"<b>{millify(y)}</b>",
                       font=dict(color=col, size=15),
                       showarrow=False)
    
    y_perc=1-((df[metric].max()-y)/(df[metric].max()-df[metric].min()))
    
    fig.add_annotation(x=1.08, y=y_perc-0.05,
                       xref="paper",
                       yref='paper',    
                       text="Average",
                       font=dict(color=col, size=15),
                       showarrow=False)
    fig.update_layout(margin=dict(r=100))

def add_avevline(fig, df, metric, col="#ffab40"):
    """
    Add a vertical line to an existing plotly figure
    input:
        x = value of vertical line
        col = color
    """
    x=df[metric].mean()
    fig.add_vline(x=x, line_width=3, line_dash="dot", line_color=col)
    fig.add_annotation(x=x, y=1.05, xref="paper",
            text=f"<b>{millify(x)}</b>",
            font=dict(color=col, size=15),
            showarrow=False)
    
    x_perc=1-((df[metric].max()-x)/(df[metric].max()-df[metric].min()))
    
    fig.add_annotation(x=x_perc, y=1.07,
                       xref="paper",
                       yref='paper',
                       text=f"Average:  <b>{millify(x)}</b>",
                       font=dict(color=col, size=15),
                       showarrow=False)

def resize_image(image, length):
    """
    Resize an image to a square. Can make an image bigger to make it fit or smaller if it doesn't fit. It also crops
    part of the image.

    :param image: Image to resize.
    :param length: Width and height of the output image.
    :return: Return the resized image.

    Resizing strategy : 
     1) We resize the smallest side to the desired dimension (e.g. 1080)
     2) We crop the other side so as to make it fit with the same length as the smallest side (e.g. 1080)
    
    source:https://stackoverflow.com/questions/43512615/reshaping-rectangular-image-to-square
    """
    if image.size[0] < image.size[1]:
        # The image is in portrait mode. Height is bigger than width.

        # This makes the width fit the LENGTH in pixels while conserving the ration.
        resized_image = image.resize((length, int(image.size[1] * (length / image.size[0]))))

        # Amount of pixel to lose in total on the height of the image.
        required_loss = (resized_image.size[1] - length)

        # Crop the height of the image so as to keep the center part.
        resized_image = resized_image.crop(
            box=(0, required_loss / 2, length, resized_image.size[1] - required_loss / 2))

        # We now have a length*length pixels image.
        return resized_image
    else:
        # This image is in landscape mode or already squared. The width is bigger than the heihgt.

        # This makes the height fit the LENGTH in pixels while conserving the ration.
        resized_image = image.resize((int(image.size[0] * (length / image.size[1])), length))

        # Amount of pixel to lose in total on the width of the image.
        required_loss = resized_image.size[0] - length

        # Crop the width of the image so as to keep 1080 pixels of the center part.
        resized_image = resized_image.crop(
            box=(required_loss / 2, 0, resized_image.size[0] - required_loss / 2, length))

        # We now have a length*length pixels image.
        return resized_image

#=============================================================================#
#                               Read data cubes                               #
#=============================================================================#

# Run functions
(months, 
    monthly_resto_users, 
    monthly_resto_images, 
    daily_resto, 
    per_cuisine, cuisine_ranking, 
    monthly_resto)=load_data(rid=rid)

# Get restaurant name
rname=monthly_resto['restaurant_name'].iloc[0]

# Aggregate metrics for user, impressions (total times the image showed up), and left swipes
@st.experimental_memo()
def filter_df(daily_resto,start_date, end_date):
    return daily_resto[(daily_resto['date'] >= start_date) & (daily_resto['date'] <= end_date)]

#=============================================================================#
#                              Streamlit proper                               #
#=============================================================================#
#After your with st.form statement… add
c1, c2 = st.columns(2, gap='medium')
with c1:
    
    # Center radio buttons
    st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)

    # Create title with logo
    LOGO_IMAGE = "cheezy_logo.JPG"

    st.markdown(
        """
        <style>
        .container {
            display: flex;
        }
        .logo-text {
            font-weight:700 !important;
            font-size:50px !important;
            color: rgba(0,77,61,100) !important;
            padding-top: 5px !important;
            padding-left: 15px !important;
        }
        .logo-img {
            float:right;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"""
        <div class="container">
            <img class="logo-img" width=100px height=75px src="data:image/png;base64,{base64.b64encode(open(LOGO_IMAGE, "rb").read()).decode()}">
            <p class="logo-text">{rname}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

#     # Header
#     st.header(f"{rname.upper()}")
    
with c2:
    st.text(' ')
    st.text(' ')
    month = st.selectbox(
    'Please select a month:',
    months)

st.markdown("""---""")

# Filter data
temp=filter_month(month=month)
labels=['f_monthly_resto_users', 'f_monthly_resto_images', 'agg', 'f_per_cuisine', 'f_cuisine_ranking', 'df']
for i,j in enumerate(temp):
    globals()[labels[i]]=j
    
# Sort agg
agg.sort_values("date",inplace=True)

# Get a list of distinct cuisines
cuisines=f_cuisine_ranking.cuisine.unique()

# Get min and max dates
min_dt=agg['date'].min()
max_dt=agg['date'].max()

# Restaurant information
rank=df['rank'].iloc[0]

# Get values per cuisine
n_per_row=3
n_rows=int(np.ceil((len(cuisines)+1)/n_per_row))

cuisine_n_resto=[]
cuisine_rid_rank=[]
cuisine_ave_users=[]
cuisine_ave_imp=[]
cuisine_ave_swipe=[]

for cuisine in cuisines:
    df_all=f_per_cuisine.query("cuisine==@cuisine")
    cuisine_n_resto.append(df_all['restos'].iloc[0])
    cuisine_rid_rank.append(f_cuisine_ranking.query("cuisine==@cuisine")['crank'].iloc[0])
    cuisine_ave_users.append(millify(df_all['dly_avg_users'].iloc[0])) # Shall we compare vs last month?
    cuisine_ave_imp.append(millify(df_all["dly_avg_impressions"].iloc[0])) # Shall we compare vs last month?
    cuisine_ave_swipe.append(millify(df_all["dly_avg_left_swipes"].iloc[0])) # Shall we compare vs last month?
    
for i in range(n_rows):
    with st.container():
        if n_per_row==2:
            c1, c2 = st.columns(n_per_row, gap='medium')
        elif n_per_row==3:
            c1, c2, c3 = st.columns(n_per_row, gap='medium')
        for j in range(n_per_row):
            if i==0 and j==0:
                with c1:
                   # Subheader
                    st.subheader(f"#{rank} among Barcelona Restaurants in Tripadvisor")
                    st.markdown('<p style="color:orange; size:13;margin:0px;"><b>Restaurant averages:</b></p>',
                                unsafe_allow_html=True)
            else:
                with globals()['c'+str(j+1)]:
                    c_ind=int(2*i+j-1)
                    try:
                        # Aggregate cuisine
                        st.subheader(f"#{cuisine_rid_rank[c_ind]} out of {cuisine_n_resto[c_ind]} {cuisines[c_ind]} Restaurants in Barcelona")
                        st.text('Industry averages:')
                    except:
                        continue
        ncols=int(3*n_per_row)
        if ncols==6:
            c1, c2, c3, c4, c5, c6 = st.columns(ncols, gap='medium')
        elif ncols==9:
            c1, c2, c3, c4, c5, c6, c7, c8, c9 = st.columns(ncols, gap='medium')
            
        for j in range(n_per_row):
            if i==0 and j==0:
                with c1:
                   # Display average daily metrics 
                    st.metric(label="Daily Average Users", value=millify(df['dly_avg_users'].iloc[0])) 
                with c2:
                    st.metric(label="Daily Average Impressions", value=millify(df["dly_avg_impressions"].iloc[0])) 
                with c3:
                    st.metric(label="Daily Average Left Swipes", value=millify(df["dly_avg_left_swipes"].iloc[0])) 
            else:
                try:
                    c_ind=int(n_per_row*i+j-1)
                    with globals()['c'+str(j*3+1)]:
                        st.metric(label="Daily Average Users", value=cuisine_ave_users[c_ind]) 

                    with globals()['c'+str(j*3+2)]:
                        st.metric(label="Daily Average Impressions", value=cuisine_ave_imp[c_ind])

                    with globals()['c'+str(j*3+3)]:
                        st.metric(label="Daily Average Left Swipes", value=cuisine_ave_swipe[c_ind])
                except:
                    continue
st.markdown('<div style="text-align: right;"><i>Note: cuisine ranking based on total number of impressions</i></div>', unsafe_allow_html=True)


st.markdown("""---""")
           
#After your with st.form statement… add
c1, c2, c3 = st.columns(3, gap='medium')
with c1:
    st.subheader("Cheezy: Daily counts")
    metric = st.radio("Select Metric: ", ('Impressions', 'Left Swipes', 'Users')).lower().replace(' ','_')
    
    # Graph for the daily count of users, impressoins, and left swipes
    fig=px.line(agg, 'date', metric, 
                color_discrete_sequence=['rgba(0,77,61,100)'], height=400, width=600)
    add_avehline(fig, agg, metric)
    
    prettify_fig(fig, xtitle=None, ytitle=metric)
    st.plotly_chart(fig, theme='streamlit', width=650)
    # st.line_chart(df, x='date', y=metric)
    
with c2:
    # Header
    st.subheader("Users: City of Residence")
    st.text(" ")
    fig=px.bar(f_monthly_resto_users.groupby('city').users.sum().sort_values(ascending=True), orientation='h', 
               color_discrete_sequence=['rgba(0,77,61,100)'],
               width=650
               )
    prettify_fig(fig, xtitle=None, ytitle=None, showlegend=False)
    st.plotly_chart(fig, theme='streamlit')

with c3:
    st.subheader("Users: Age")
    st.text(" ")
    fig=px.bar(f_monthly_resto_users.assign(age_bucket=lambda x: 5*2+x.age_bucket)
               .groupby('age_bucket')
               .users.sum()
               .sort_values(ascending=True), 
               color_discrete_sequence=['rgba(0,77,61,100)'],
               width=600
               )
    prettify_fig(fig, xtitle='Age', ytitle='User count', showlegend=False)
    
    st.plotly_chart(fig, theme='streamlit')

st.markdown("""---""")

a1, a2 = st.columns(2, gap='medium')
with a1:
    st.subheader("Top 3 left-swiped images")
with a2:
    st.subheader("Bottom 3 left-swiped images")

c1, c2, c3, c4, c5, c6 = st.columns(6, gap='medium')

# Display top 3 images
top3img=f_monthly_resto_images[f_monthly_resto_images['rank'].str.startswith('top-')]
for i, n, s, c in zip(top3img['file'], top3img['name'], top3img['left_swipes'], [c1,c2,c3]):
    with c:
        img=spark_read_image(img_file=image_path+'/'+i)
        st.image(resize_image(img, 400), width=280)
        st.text(f"{n}: {(s)}")

# bottom top 3 images
bottom3img=f_monthly_resto_images[f_monthly_resto_images['rank'].str.startswith('bot-')]
bottom3img=bottom3img[~bottom3img.file.isin(top3img.file.unique())]    
for i, n, s, c in zip(bottom3img['file'], bottom3img['name'], bottom3img['left_swipes'], [c4,c5,c6]):
    with c:
        img=spark_read_image(img_file=image_path+'/'+i)
        st.image(resize_image(img, 400), width=280)
        st.text(f"{n}: {(s)}")
            
