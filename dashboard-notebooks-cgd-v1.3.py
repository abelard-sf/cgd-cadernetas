# https://community.plotly.com/t/solved-has-anyone-made-a-date-range-slider/6531/9


#   ------------------------------------------------------------------------------------------------------------
#   ---     import dependencies
#   ------------------------------------------------------------------------------------------------------------
import json
import datetime
from datetime import datetime

import dash
import numpy as np
import pandas as pd
from dash import Dash
from dash import html
from dash import dcc
from dash import dash_table
import dash_bootstrap_components as dbc
from dash import Output, Input
from dash.exceptions import PreventUpdate
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

from os import listdir, path
from os.path import isfile, join
from dateutil.relativedelta import relativedelta


#   ------------------------------------------------------------------------------------------------------------
#   ---     types, constants & variables
#   ------------------------------------------------------------------------------------------------------------
month_mapping = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
                 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
                 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}


day_of_week_mapping = {0: 'Segunda-feira', 1: 'Terça-feira', 3: 'Quarta-feira',
                       4: 'Quinta-feira', 5: 'Sexta-feira',
                       6: 'Sábado', 7: 'Domingo'}

app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1.0'}])

server = app.server

FILES_TO_PROCESS_FOLDER = 'FILES_TO_PROCESS'

#   ------------------------------------------------------------------------------------------------------------
#   ---     types, constants & variables
#   ------------------------------------------------------------------------------------------------------------

#   **************************************************************************************
#   split file name
#   **************************************************************************************
def split_file_name(file_name):
    first_file_token = file_name[:file_name.find('-')]
    rest_of_str = file_name[file_name.find('-') + 1:]

    second_file_token = rest_of_str[:rest_of_str.find('-')]
    third_file_token = rest_of_str[rest_of_str.find('-') + 1:]
    third_file_token = third_file_token[:-4]

    return first_file_token, second_file_token, third_file_token


#   **************************************************************************************
def get_dataset():
    onlyfiles = [f for f in listdir(FILES_TO_PROCESS_FOLDER) if isfile(join(FILES_TO_PROCESS_FOLDER, f))]

    dataset = None

    for file_name in onlyfiles:
        first_file_token, second_file_token, third_file_token = split_file_name(file_name)

        print(f'Processing file: {file_name}')

        # read file
        df = pd.read_csv(join(FILES_TO_PROCESS_FOLDER, file_name), sep='|', header=None)
        df.columns = ['date', 'time', 'nr_try', 'reply_data', 'reply_code']

        # set new column for notebook reader id
        notebook_reader = third_file_token
        df['notebook_reader'] = notebook_reader

        # make nr_try start at 1
        df['nr_try'] = df['nr_try'] + 1

        # datetime features
        df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df['date'] = df['date'].astype('datetime64[ns]')
        df['hour'] = df['date_time'].astype('datetime64[ns]')

        # reorder columns
        df = df[['date_time', 'date', 'time', 'hour', 'notebook_reader', 'nr_try', 'reply_data', 'reply_code']]

        if dataset is None:
            # print('dataset created')
            dataset = df

        else:
            # print('appending to dataset')
            dataset = pd.concat([dataset, df])

    dataset = dataset.set_index('date_time')

    return dataset


#   **************************************************************************************
def get_monthly_marks(df):
    # extract unique year/month/day combinations as a PeriodIndex
    df = df.sort_index()
    months = df.index.to_period("M").unique()
    print(f'MONTHS: {months}')

    # convert PeriodIndex to epoch series and YYYY-MM string series
    epochs = months.to_timestamp().astype(np.int64) // 10**9
    strings = months.strftime("%Y-%m")

    return dict(zip(epochs, strings))


#   **************************************************************************************
def get_weekly_marks(df):
    # extract unique year/month/day combinations as a PeriodIndex
    df = df.sort_index()
    weeks = df.index.to_period("W").unique()
    print(f'WEEKS: {weeks}')

    # convert PeriodIndex to epoch series and YYYY-MM-DD string series
    epochs = weeks.to_timestamp().astype(np.int64) // 10**9
    strings = weeks.strftime("%Y-%m-%d")

    return dict(zip(epochs, strings))


#   **************************************************************************************
def get_daily_marks(df):
    # extract unique year/month/day combinations as a PeriodIndex
    df = df.sort_index()
    days = df.index.to_period("D").unique()
    print(f'DAYS: {days}')

    # convert PeriodIndex to epoch series and YYYY-MM-DD string series
    epochs = days.to_timestamp().astype(np.int64) // 10**9
    strings = days.strftime("%Y-%m-%d")

    return dict(zip(epochs, strings))


#   **************************************************************************************
def get_msg_initial_period():
    dataset_min_date = df['date'].min()
    dataset_max_date = df['date'].max()

    msg_initial_period = f'de {dataset_min_date} a {dataset_max_date}'

    return msg_initial_period


#   **************************************************************************************
def get_notebook_readers_per_day(df_in):
    notebook_readers_per_day = pd.DataFrame(df_in.groupby(by='date').agg('notebook_reader').unique().sort_index())
    notebook_readers_per_day.index = notebook_readers_per_day.index.astype('datetime64[ns]')

    notebook_readers_per_day['nr_notebook_readers'] = [len(notebook_reader) for notebook_reader in notebook_readers_per_day['notebook_reader']]

    return notebook_readers_per_day


#   **************************************************************************************
def get_readings_per_day(df_in):
    readings_per_day = df_in.groupby(by='date').agg(nr_readings=('notebook_reader', 'count'))

    return readings_per_day


def get_unsuccessful_readings_per_day(df_in):
    unsuccessful_readings_per_day = df_in[df_in['reply_code'] == 1]
    unsuccessful_readings_per_day = unsuccessful_readings_per_day.groupby(by='date').agg(nr_unsuccessful_readings=('notebook_reader', 'count'))

    return unsuccessful_readings_per_day


#   **************************************************************************************
def get_plot_readings_per_period(df_in):
    notebook_readers_per_day = get_notebook_readers_per_day(df_in)
    readings_per_day = get_readings_per_day(df_in)
    unsuccessful_readings_per_day = get_unsuccessful_readings_per_day(df_in)

    readings_per_day = readings_per_day.join(notebook_readers_per_day['nr_notebook_readers'])
    readings_per_day = readings_per_day.join(unsuccessful_readings_per_day['nr_unsuccessful_readings'])

    readings_per_day['average_readings_per_day'] = round(readings_per_day['nr_readings'] / readings_per_day['nr_notebook_readers'], 2)
    readings_per_day['average_unsuccessful_readings_per_day'] = round(readings_per_day['nr_unsuccessful_readings'] / readings_per_day['nr_notebook_readers'], 2)

    y_limit = int(readings_per_day['average_readings_per_day'].max() * 1.20)

    # fig = px.bar(data_frame=readings_per_day,
    #              x=readings_per_day.index,
    #              y='average_readings_per_day',
    #              text='average_readings_per_day',
    #              range_y=[0, y_limit])

    fig = go.Figure(data=[
        go.Bar(name='Média Diária de Leituras',
               x=readings_per_day.index,
               y=readings_per_day['average_readings_per_day'],
               text=readings_per_day['average_readings_per_day'],
               marker_color='#5CAEDF'
               ),

        go.Bar(name='Média Diária de Erros',
               x=readings_per_day.index,
               y=readings_per_day['average_unsuccessful_readings_per_day'],
               text=readings_per_day['average_unsuccessful_readings_per_day'],
               marker_color='#F54A4A')

    ])
    # Change the bar mode
    fig.update_layout(barmode='group')

    fig.update_layout(legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="right",
        x=1
    ))

    # fig.layout.title = f"<b><span style='font-size:1.2em;color:#2767F1'>Média de Leituras</span></b>"
    # fig.layout.xaxis.title = "<b><span style='font-size:1.0em;color:#2767F1'>Data</span></b>"
    # fig.layout.yaxis.title = "<b><span style='font-size:1.0em;color:#2767F1'>Leituras</span></b>"
    fig.layout.title = ""
    fig.layout.xaxis.title = ""
    fig.layout.yaxis.title = ""

    # change the color of the bars
    # fig.update_traces(marker_color='#5CAEDF')
    fig.update_traces(textfont_size=12, textangle=0, cliponaxis=False)

    return fig


#   **************************************************************************************
def count_total_notebook_readers(df_in):
    return df_in['notebook_reader'].nunique()


#   **************************************************************************************
def get_kpi_nr_notebook_readers(df_in):

    kpi = html.Div([
        html.Div(html.P(count_total_notebook_readers(df_in), style={'font-size':'5.0em','color':'#5CAEDF', 'text-align':'center',
                                                      'font-weight':'750', 'padding':0, 'margin-top':-10})),
        html.Div('LEITORES', style={'font-size':'1.5em','color':'#2067DC', 'text-align':'center', 'font-weight':'750',
                                   'padding':0, 'margin-top':-40, 'width':'100%'})
    ], style={'margin-left':0, 'align':'center', 'width':'20%'})
    return kpi


#   **************************************************************************************
def count_total_readings(df_in):
    return len(df_in)


#   **************************************************************************************
def get_kpi_total_readings(df_in):

    kpi = html.Div([
        html.Div(html.P(count_total_readings(df_in), style={'font-size':'5.0em','color':'#5CAEDF', 'align':'center',
                                                      'font-weight':'750', 'padding':0, 'margin-top':-10})),
        html.Div('LEITURAS', style={'font-size':'1.5em','color':'#2067DC', 'align':'center', 'font-weight':'750',
                                   'padding':0, 'margin-top':-40, 'width':'100%'})
    ], style={'margin-left':0, 'align':'center', 'width':'20%'})
    return kpi


#   **************************************************************************************
def count_unsuccessful_readings(df_in):
    return len(df_in[df_in['reply_code'] == 1])


#   **************************************************************************************
def get_kpi_percent_reading_errors(df_in):
    total_readings = count_total_readings(df_in)
    total_unsuccessful_readings = count_unsuccessful_readings(df_in)

    if total_readings > 0:
        kpi_percent_reading_errors = str(round(((total_unsuccessful_readings / total_readings) * 100), 1))

        kpi_percent_reading_errors = f'{kpi_percent_reading_errors}%'
    else:
        kpi_percent_reading_errors = '0%'

    kpi = html.Div([
        html.Div(html.P(kpi_percent_reading_errors, style={'font-size': '5.0em', 'color':'#F54A4A', 'align': 'center',
                                                            'font-weight': '750', 'padding': 0, 'margin-top': -10})),

        html.Div('ERROS', style={'font-size': '1.5em', 'color':'#F54A4A', 'text-align': 'right', 'font-weight': '750',
                                    'padding': 0, 'margin-top': -40, 'width': '100%'})
    ], style={'margin-left': 0, 'align': 'center', 'width': '20%'})

    return kpi


#   **************************************************************************************
def count_unique_notebooks(df_in):
    return df_in[df_in['reply_code'] == 0]['reply_data'].nunique()


#   **************************************************************************************
def get_kpi_unique_notebooks(df_in):
    kpi = html.Div([
        html.Div(html.P(count_unique_notebooks(df_in), style={'font-size':'5.0em','color':'#5CAEDF', 'align':'center',
                                                      'font-weight':'750', 'padding':0, 'margin-top':-10})),
        html.Div('CADERNETAS', style={'font-size':'1.5em','color':'#2067DC', 'align':'center', 'font-weight':'750',
                                   'padding':0, 'margin-top':-40, 'width':'100%'})
    ], style={'margin-left':0, 'align':'center', 'width':'20%'})

    return kpi


#   -----------------------------------------------------------------------------------------



#   ------------------------------------------------------------------------------------------------------------
#   ---     application
#   ------------------------------------------------------------------------------------------------------------

#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
#   get data
#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
df = get_dataset()
# print(df.info())

dataset_min_date = df['date'].min()
dataset_max_date = df['date'].max()

start_date = pd.Timestamp(dataset_min_date)
end_date = pd.Timestamp(dataset_max_date)



#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
#   +++     layout
#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
app.layout = html.Div([
        # DASHBOARD TITLE
        html.Div([
            html.Div(children='CGD - CADERNETAS',
                     style={'font-size':'5.0em','color':'#2067DC', 'text-align':'left',
                            'font-weight':'750', 'margin-bottom':20})
        ], style={'margin-top':10, 'margin-left':40, 'padding':0}),

        # SELECTED PERIOD
        # html.Div(id='msg_selected_period', children=get_msg_initial_period(),
        #          style={'font-size':'1.75em','color':'#229FE6', 'text-align':'right',
        #                 'font-weight':'750', 'margin-top':-100, 'margin-right':100, 'padding':0}),

        # KPIs
        html.Div([
            html.Div(id='kpi_nr_notebook_readers', children=get_kpi_nr_notebook_readers(df),
                     style={'float':'left', 'width':'20%', 'margin-left':100, 'text-align':'right'}),

            html.Div(id='kpi_total_readings', children=get_kpi_total_readings(df),
                     style={'float':'left', 'width':'20%', 'margin-left':50}),

            html.Div(id='kpi_percent_reading_errors', children=get_kpi_percent_reading_errors(df),
                     style={'float': 'left', 'width': '20%', 'margin-left': 50, 'text-align':'right'}),

            html.Div(id='kpi_unique_notebooks', children=get_kpi_unique_notebooks(df),
                     style={'float': 'left', 'width': '20%', 'margin-left': 50})
        ], style={'margin-top':10}),

        html.Div([
            dcc.Graph(id='plot_readings_per_period', figure=get_plot_readings_per_period(df))
        ], style={'margin-top':165, 'margin-left':10, 'padding':-10, 'float':'top'}),

        html.Div([
            html.Label('SELECIONE PERIODO',  style={'font-size':'1.5em','color':'#5CAEDF',
                                                    'text-align':'left', 'font-weight':'750',
                                                    'margin-left':30}),
            dcc.RangeSlider(
                updatemode='mouseup',
                allowCross=False,
                id="date_slider",
                min=pd.Timestamp(df.index.min()).timestamp(),
                max=pd.Timestamp(df.index.max()).timestamp(),
                marks = get_monthly_marks(df),
                # tooltip={"placement": "bottom", "always_visible": True}
            )

        ],style={"width": "92%", 'margin-top':5, 'margin-left':80})

])


#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
#   +++     callbacks
#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
@app.callback(
    # Output('msg_selected_period', 'children'),
    Output('plot_readings_per_period', 'figure'),
    Output('kpi_nr_notebook_readers', 'children'),
    Output('kpi_total_readings', 'children'),
    Output('kpi_percent_reading_errors', 'children'),
    Output('kpi_unique_notebooks', 'children'),
    Input('date_slider', 'value')
)

def show_info(date_slider_value):
    if date_slider_value is None:
        raise PreventUpdate

    interval_start_date = datetime.fromtimestamp(date_slider_value[0])
    interval_end_date = datetime.fromtimestamp(date_slider_value[1])

    print(f'PERIODO: {interval_start_date} - {interval_end_date}')

    # msg_selected_period = f'de {interval_start_date} a {interval_end_date}'

    df_selected_period = df[(df.index >= interval_start_date) & (df.index <= interval_end_date)]
    # print(f'Readings: {len(df_selected_period)}')

    if len(df_selected_period) > 0:
        fig = get_plot_readings_per_period(df_selected_period)
    else:
        fig = dash.no_update

    kpi_nr_notebooks = get_kpi_nr_notebook_readers(df_selected_period)
    kpi_total_readings = get_kpi_total_readings(df_selected_period)
    kpi_percent_reading_errors = get_kpi_percent_reading_errors(df_selected_period)
    kpi_unique_notebooks = get_kpi_unique_notebooks(df_selected_period)

    # return msg_selected_period, fig, kpi_nr_notebooks, kpi_total_readings, kpi_percent_reading_errors
    return fig, kpi_nr_notebooks, kpi_total_readings, kpi_percent_reading_errors, kpi_unique_notebooks


#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
#   application startup
#   ++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':
    app.run_server(debug=True, port=8057)
