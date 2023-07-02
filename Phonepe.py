import pandas as pd
import mysql.connector
import numpy as np
import os
from pathlib import Path
import json
from pandas.io import sql
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st


class phonepe():
    

    def fetch_data_from_json(self,loc):

        path = loc
        #path = "/Users/potnurusainishant/Desktop/Python/Guvi Projects/Phonepe Pulse/pulse/data/map/transaction/hover/country/india/state"
        final_df = pd.DataFrame()
        for state in os.listdir(path=path):
            a = state
            for year in os.listdir(path= path+"/"+state):
                for files in os.listdir(path=path+"/"+state+"/"+year):
                    with open(path+"/"+state+"/"+year+"/"+files,'r',encoding="utf8") as file:
                        json_data = json.load(file)

                    dat = json_data['data']['hoverDataList']
                    df = pd.json_normalize(dat,'metric',['name'],record_prefix='metric_')
                    df.insert(0,"state",state)
                    df.insert(1,"year",year)
                    df.insert(2,"quarter",files)
            
                    final_df = pd.concat([df,final_df],ignore_index=False)

        return final_df
    
    def clean_df(self,df):
        
        df.rename(columns = {'name':'district_name','metric_count':'transactions'},inplace = True)
        df.drop('metric_type',axis=1,inplace = True)
        #change the quarter format in the quarter column
        df['quarter'] = 'Q'+df['quarter'].astype(str)
        df['quarter'] = df['quarter'].str.replace('.json',"")
        df['state'] = df['state'].str.replace('-'," ")
        df['state'] = df['state'].str.title()
        df['state'] = df['state'].str.replace('Andaman & Nicobar Islands','Andaman & Nicobar Island')
        df['state'] = df['state'].str.replace('Delhi','NCT of Delhi')
        df['state'] = df['state'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Daman & Diu')
        df['state'] = df['state'].str.replace('Arunachal Pradesh','Arunanchal Pradesh')
        df = df[df['state']!= 'Ladakh']
        df.insert(0,"Country","India")
        return df

    def migrate_to_sql(self,df):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="nishantsai8@P",
        database = "phonepe"
        )
        mycursor = mydb.cursor()
        for row,col in df.iterrows():

            country = col[0]
            state = col[1]
            year = col[2]
            quarter = col[3]
            transactions = col[4]
            metric_amount = col[5]
            district_name = col[6]

            query = """insert into report (country,state,year,quarter,transactions,metric_amount,district_name) 
                    values (%s,%s,%s,%s,%s,%s,%s)"""

            record = [country,state,year,quarter,transactions,metric_amount,district_name]

            mycursor.execute(query,record)
        
        mydb.commit()
        mycursor.close()
    
    def fetch_data_db(self):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="nishantsai8@P",
        database = "phonepe"
        )

        query = """Select * from report"""

        data = pd.read_sql(query,mydb)
        
        return data

    def transform(self,df_cleaned,indian_states):
        state = df_cleaned.groupby(['state','year','quarter']).aggregate({'metric_amount':'sum','transactions':'sum'}).reset_index()
    
        state_id_map = {}
        for feature in indian_states['features']:
            feature['id'] = feature['properties']['state_code']
            state_id_map[feature['properties']['st_nm']] = feature['id']
        state['id'] = state['state'].apply(lambda x: state_id_map[x])
        return(state)
 
    def visual_app(self,data,geo):

        st.title("Phonepe Pulse")
        col1,col2  = st.columns(2)
        with col1:
            year = st.selectbox("Select Year",data.year.unique())
        with col2:
            quarter = st.radio("Select Quarter",data.quarter.unique(),horizontal=True)
        
        data = data[(data['year']==year) & (data['quarter']==quarter)]
        fig = px.choropleth(data,
                  locations= 'id',geojson= geo,color='metric_amount')
        fig.update_layout(legend_title = "Amount")
        fig.update_geos(fitbounds = "locations",visible = False)
        #fig.show()

        st.plotly_chart(fig,use_container_width=True)


if __name__ == '__main__':

    obj = phonepe()
    df = obj.fetch_data_from_json(loc="/Users/potnurusainishant/Desktop/Python/Guvi Projects/Phonepe Pulse/pulse/data/map/transaction/hover/country/india/state")
    df_cleaned = obj.clean_df(df)
    df_migrate = obj.migrate_to_sql(df_cleaned)
    data = obj.fetch_data_db()
    india_states = json.load(open("/Users/potnurusainishant/Desktop/Python/Guvi Projects/Phonepe Pulse/states_india.geojson","r"))
    state= obj.transform(data,india_states)
    visual_plot = obj.visual_app(state,india_states)



    


