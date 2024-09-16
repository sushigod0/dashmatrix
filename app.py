import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import json
from datetime import datetime, timedelta

# Load data from JSON file
with open('metric_table.json', 'r') as file:
    data = json.load(file)

# Convert dict to list and parse dates
data_list = []
for _, entry in data.items():
    entry['date'] = datetime.strptime(entry['date'], '%Y-%m-%d').date()
    data_list.append(entry)

#sort the list
data_list.sort(key=lambda x: x['date'], reverse=True)
# Get the last 7 days of data
filtered_data = data_list[:7]
filtered_data.reverse()  # Reverse to show oldest to newest

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout
app.layout = html.Div([
    html.H1('Metrics Dashboard'),
    dcc.Graph(id='tp-chart'),
    dcc.Graph(id='tn-chart')
])

# Callback to update the charts
@app.callback(
    [Output('tp-chart', 'figure'),
     Output('tn-chart', 'figure')],
    Input('tp-chart', 'id')
)
def update_graphs(id):
    # Prepare data for the charts
    dates = [entry['date'] for entry in filtered_data]
    tp = [entry['TP'] for entry in filtered_data]
    tp_fn = [entry['TP'] + entry['FN'] for entry in filtered_data]
    tn = [entry['TN'] for entry in filtered_data]
    tn_fp = [entry['TN'] + entry['FP'] for entry in filtered_data]

    #prepare ratios for the chart
    percent_scam = [tp[i]/tp_fn[i] if tp_fn[i]!=0 else 0 for i in range(len(tp))]
    percent_non_scam = [tn[i]/tn_fp[i] if tn_fp[i]!=0 else 0 for i in range(len(tn))]

    # Create the TP chart
    tp_trace = go.Bar(x=dates, y=tp, name='Scam Predicted', text=tp, textposition='auto', hoverinfo='x+y')
    tp_fn_trace = go.Bar(x=dates, y=tp_fn, name='Scam Actual', text=tp_fn, textposition='auto', hoverinfo='x+y')
    recall_ratio_trace = go.Scatter(
        x=dates, y=percent_scam,
        name='Scams Percent',
        mode='lines+markers+text',
        text=[f'{r:.2f}' for r in percent_scam],
        textposition='top center',
        yaxis='y2',
        hoverinfo='x+y'
    )
    tp_layout = go.Layout(
        title='Scam Prediction for Last 7 Days',
        barmode='group',
        xaxis_title='Date',
        yaxis_title='Count',
        yaxis2=dict(
            title='Scam Percent',
            overlaying='y',
            side='right'
        ),
        legend=dict(orientation='h', y = 1.1)
    )

    tp_figure = {'data': [tp_trace, tp_fn_trace, recall_ratio_trace], 'layout': tp_layout}

    # Create the TN chart
    tn_trace = go.Bar(x=dates, y=tn, name='Predicted Non-Scams')
    tn_fp_trace = go.Bar(x=dates, y=tn_fp, name='Actual Non-Scams')
    tnr_ratio_trace = go.Scatter(
        x=dates,
        y=percent_non_scam,
        name='Non-Scam Percent',
        mode='lines+markers+text',
        text=[f'{r:.2f}' for r in percent_non_scam],
        textposition='top center',
        yaxis='y2',
        hoverinfo='x+y'
    )

    tn_layout = go.Layout(
        title='Non-Scam Prediction for Last 7 Days',
        barmode='group',
        xaxis_title='Date',
        yaxis_title='Count',
        yaxis2=dict(
            title='Non_Scam Percent',
            overlaying='y',
            side='right'
        ),
        legend=dict(orientation='h', y=1.1)
    )

    tn_figure = {'data': [tn_trace, tn_fp_trace, tnr_ratio_trace], 'layout': tn_layout}

    return tp_figure, tn_figure

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='127.0.0.1', port=8080)