import dash
from dash import dcc, html, dash_table, Input, Output
import pandas as pd
import plotly.graph_objs as go
from data.utils.database_service import DatabaseService

app = dash.Dash(__name__)
app.title = "CCIL Securities Dashboard"

# Azure Web App Configuration
application = app 

# Global CSS style
CARD_STYLE = {
    "backgroundColor": "white",
    "padding": "20px",
    "borderRadius": "12px",
    "boxShadow": "0 2px 6px rgba(0,0,0,0.15)",
    "marginBottom": "20px"
}

# Layout
app.layout = html.Div([
    # Title
    html.H1(
        "CCIL Securities Dashboard",
        style={
            'textAlign': 'center',
            'color': '#2c3e50',
            'marginBottom': '30px',
            'fontFamily': 'Arial, sans-serif'
        }
    ),

    # Dropdown
    html.Div([
        html.Label("Select Security:", style={'fontWeight': 'bold'}),
        dcc.Dropdown(
            id='security-dropdown',
            clearable=False,
            style={"width": "50%"}
        )
    ], style={**CARD_STYLE, "width": "50%", "margin": "0 auto"}),

    # Graphs row
    html.Div([
        html.Div([dcc.Graph(id='ttc-chart')], style={**CARD_STYLE, "flex": "1", "marginRight": "10px"}),
        html.Div([dcc.Graph(id='tta-chart')], style={**CARD_STYLE, "flex": "1", "marginLeft": "10px"})
    ], style={"display": "flex", "flexDirection": "row", "width": "90%", "margin": "0 auto", "marginTop": "20px"}),

    # Table
    html.Div([
        dash_table.DataTable(
            id='data-table',
            page_size=10,
            style_table={'overflowX': 'auto'},
            style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
            style_cell={'padding': '8px', 'textAlign': 'left'},
            style_data={'border': '1px solid #ddd'}
        )
    ], style={**CARD_STYLE, "width": "90%", "margin": "0 auto"}),

    # Interval for auto-refresh every 30 minutes
    dcc.Interval(
        id='interval-refresh',
        interval=30*60*1000,  # 30 minutes in milliseconds
        n_intervals=0
    )
], style={"backgroundColor": "#f4f6f7", "padding": "20px"})

# Callback to update everything
@app.callback(
    [Output('ttc-chart', 'figure'),
     Output('tta-chart', 'figure'),
     Output('data-table', 'data'),
     Output('security-dropdown', 'options'),
     Output('security-dropdown', 'value')],
    [Input('security-dropdown', 'value'),
     Input('interval-refresh', 'n_intervals')]
)
def update_dashboard(selected_security, n_intervals):
    # Fetch fresh data
    db = DatabaseService()
    df = pd.DataFrame(db.fetch_data("SELECT * FROM ccil_securities"))
    if 'download_timestamp' in df.columns:
        df['download_timestamp'] = pd.to_datetime(df['download_timestamp'])

    # Update dropdown options
    security_options = df['ismt_idnt'].unique() if 'ismt_idnt' in df.columns else []
    dropdown_value = selected_security if selected_security in security_options else (security_options[0] if len(security_options) > 0 else None)
    dropdown_options = [{'label': sec, 'value': sec} for sec in security_options]

    if not dropdown_value:
        return go.Figure(), go.Figure(), [], dropdown_options, dropdown_value

    filtered = df[df['ismt_idnt'] == dropdown_value].sort_values('download_timestamp')

    # TTC Chart
    ttc_fig = go.Figure()
    if 'download_timestamp' in filtered.columns and 'ttc' in filtered.columns:
        ttc_fig.add_trace(go.Scatter(
            x=filtered['download_timestamp'],
            y=filtered['ttc'],
            mode='lines+markers',
            name='Trades',
            line=dict(color="#2980b9")
        ))
        ttc_fig.update_layout(
            title=f"Trades (TTC) for <b>{dropdown_value}</b>",
            xaxis_title="Date",
            yaxis_title="Trades",
            template="plotly_white"
        )

    # TTA Chart
    tta_fig = go.Figure()
    if 'download_timestamp' in filtered.columns and 'tta' in filtered.columns:
        tta_fig.add_trace(go.Scatter(
            x=filtered['download_timestamp'],
            y=filtered['tta'],
            mode='lines+markers',
            name='TTA',
            line=dict(color="#27ae60")
        ))
        tta_fig.update_layout(
            title=f"TTA for <b>{dropdown_value}</b>",
            xaxis_title="Date",
            yaxis_title="TTA",
            template="plotly_white"
        )

    return ttc_fig, tta_fig, filtered.sort_values('download_timestamp', ascending=False).to_dict('records'), dropdown_options, dropdown_value

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8000)
