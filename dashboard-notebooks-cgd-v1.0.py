import json

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

month_mapping = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
                 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
                 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}


day_of_week_mapping = {0: 'Segunda-feira', 1: 'Terça-feira', 3: 'Quarta-feira',
                       4: 'Quinta-feira', 5: 'Sexta-feira',
                       6: 'Sábado', 7: 'Domingo'}


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
           meta_tags=[{'name': 'viewport',
                       'content': 'width=device-width, initial-scale=1.0'}])

server = app.server

FILES_TO_PROCESS_FOLDER = 'FILES_TO_PROCESS'


def split_file_name(file_name):
    first_file_token = file_name[:file_name.find('-')]
    rest_of_str = file_name[file_name.find('-') + 1:]

    second_file_token = rest_of_str[:rest_of_str.find('-')]
    third_file_token = rest_of_str[rest_of_str.find('-') + 1:]
    third_file_token = third_file_token[:-4]

    return first_file_token, second_file_token, third_file_token


# processes all files, returns a dataframe
def get_dataset():
    onlyfiles = [f for f in listdir(FILES_TO_PROCESS_FOLDER) if isfile(join(FILES_TO_PROCESS_FOLDER, f))]

    dataset = None

    for file_name in onlyfiles:
        first_file_token, second_file_token, third_file_token = split_file_name(file_name)

        print(f'File name: {file_name}')
        # print(f'First file token: {first_file_token}')
        # print(f'Second file token: {second_file_token}')
        # print(f'Third file token: {third_file_token}')

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
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['hour'] = pd.to_datetime(df['date_time']).dt.hour

        # reorder columns
        df = df[['date_time', 'date', 'time', 'hour', 'notebook_reader', 'nr_try', 'reply_data', 'reply_code']]

        if dataset is None:
            # print('dataset created')
            dataset = df

        else:
            # print('appending to dataset')
            dataset = pd.concat([dataset, df])

    return dataset


#   -----------------------------------------------------------------------------------------

def plot_track_readings():
    x_labels_mapping = {'1': '1 Tentativa', '2': '2 Tentativas', '3': '3 Tentativas', '4': '4 Tentativas'}

    df_track_readings = pd.DataFrame(df['nr_try'].value_counts().sort_index())
    df_track_readings = df_track_readings.reset_index()
    df_track_readings.columns = ['nr_try', 'count']
    df_track_readings['nr_try'] = df_track_readings['nr_try'].astype(str)

    y_limit = int(df_track_readings['count'].max() * 1.10)
    fig = px.bar(data_frame=df_track_readings, x='nr_try', y='count', range_y=[0, y_limit], text_auto=True)
    fig.layout.title = f'Leituras de Caderneta'
    fig.layout.xaxis.title = 'Tentativas'
    fig.layout.yaxis.title = 'Quantidade'

    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=[1, 2, 3, 4],
            ticktext=['1 Tentativa', '2 Tentativas', '3 Tentativas', '4 Tentativas']
        )
    )

    return fig

def plot_notebook_readings(notebook_reader):

    df_notebook_reader = df[df['notebook_reader'] == notebook_reader]

    min_date = df_notebook_reader['date'].min()
    max_date = df_notebook_reader['date'].max()

    print(f'Notebook Reader: {notebook_reader}')
    print(f'Min Date: {min_date}')
    print(f'Max Date: {max_date}')

    df_track_readings = pd.DataFrame(df_notebook_reader['nr_try'].value_counts().sort_index())
    df_track_readings = df_track_readings.reset_index()
    df_track_readings.columns = ['nr_try', 'count']
    df_track_readings['nr_try'] = df_track_readings['nr_try'].astype(str)

    y_limit = int(df_track_readings['count'].max() * 1.10)
    fig = px.bar(data_frame=df_track_readings, x='nr_try', y='count', range_y=[0, y_limit], text_auto=True)
    fig.layout.title = "<b><span style='font-size:1.0em;color:#2767F1';'text-align':'center'>Leituras Efetuadas</span></b>"
    fig.layout.xaxis.title = "<b><span style='font-size:0.9em;color:#2767F1'>Tentativas</span></b>"
    fig.layout.yaxis.title = "<b><span style='font-size:0.9em;color:#2767F1'>Quantidade</span></b>"

    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=[1, 2, 3, 4],
            ticktext=['1 Tentativa', '2 Tentativas', '3 Tentativas', '4 Tentativas']
        )
    )

    return fig



def plot_track_readings_per_day():
    day_of_week_mapping = {'0': 'Segunda', '1': 'Terça', '2': 'Quarta', '3': 'Quinta', '4': 'Sexta', '5': 'Sábado',
                           '6': 'Domingo'}

    df_daily = df.resample('1D', on='date_time').count()
    df_daily = df_daily['reply_code']
    df_daily = df_daily.reset_index()
    df_daily.columns = ['date_time', 'count']
    df_daily['day_of_week'] = df_daily['date_time'].dt.weekday.astype(str).map(day_of_week_mapping)
    df_daily['date_time'] = df_daily['date_time'].astype(str)

    y_limit = int(df_daily['count'].max() * 1.10)
    fig = px.bar(data_frame=df_daily, x='date_time', y='count', range_y=[0, y_limit], text_auto=True)
    fig.layout.title = "<b><span style='font-size:1.3em;color:#2767F1'>Leituras por Dia</span></b>"
    fig.layout.xaxis.title = "<b><span style='font-size:1.1em;color:#2767F1'>Data</span></b>"
    fig.layout.yaxis.title = "<b><span style='font-size:1.1em;color:#2767F1'>Quantidade</span></b>"

    fig.update_layout(
        xaxis_tickformat='%d %B (%a)<br>%Y'
    )

    return fig



def info_track_readings():
    df_track_readings = pd.DataFrame(df['nr_try'].value_counts().sort_index())
    df_track_readings = df_track_readings.reset_index()
    df_track_readings.columns = ['nr_try', 'count']
    df_track_readings['nr_try'] = df_track_readings['nr_try'].astype(str)

    try_2 = df_track_readings.iloc[2]['count']

    return try_2


# read full dataset
df = get_dataset()



# --------------------------------------------------------------------------------
# --    LAYOUT
# --------------------------------------------------------------------------------
app.layout = dbc.Container([
    html.Br(),

    dbc.Row([
        dbc.Col([html.Img(src=r'assets/logo_cgd_480x380.png', width=180, height=140)], width=3),
        dbc.Col([
            dbc.Row([html.Br()]),
            dbc.Row([html.H1('Cadernetas', style={'text-align':'left', 'font-size': 60, 'color': '#0d198f'})]),
            dbc.Row([]),
        ], width=9)
    ]),

    html.Br(),

    dbc.Row([
        dbc.Col([
            html.Label('Leitor', style={'color': '#0d198f', 'font-size':'1.5em', 'font-weight': 'bold'}),

            dcc.Dropdown(id='selected_notebook_reader',
                         options=[{'label': notebook_reader, 'value': notebook_reader} for notebook_reader in df['notebook_reader'].sort_values().unique()]),

            dcc.Graph(id='track_readings_plot',
                      config={'displayModeBar': False},
                      style={'width': '20rem', 'height': '30rem'},
                      figure={'layout': go.Layout(xaxis={'showgrid': False, 'visible': False},
                                                  yaxis={'showgrid': False, 'visible': False})
                              }
                      )
        ], width=3),

        dbc.Col([], width=1),

        dbc.Col([
            html.Label('Leituras Efetuadas', style={'color': '#0d198f', 'font-size':'1.5em', 'font-weight': 'bold'}),
            dbc.Col([dcc.Graph(id='track_readings_per_day_plot', figure=plot_track_readings_per_day())], width=12)
        ], width=8)
    ]),

])

@app.callback(
    Output('track_readings_plot', 'figure'),
    Input('selected_notebook_reader', 'value')
)
def get_notebook_reader_stats(notebook_reader):

    if notebook_reader is None:
        raise PreventUpdate

    fig = plot_notebook_readings(notebook_reader)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True, port=8057)
