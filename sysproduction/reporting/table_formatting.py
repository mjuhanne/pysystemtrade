import pandas as pd
from syscore.objects import resolve_function

table_formatters = {
    "Status and time to roll in days" : "roll_report_table_formatting",
    "Optimal positions" : "optimal_positions_table_formatting",
}

def create_html_table(df, formatting_df):
    
    d = '<table border=1><thead><tr style="text-align:right"><th></th>'
    for column in df.columns:
        d += '<th>' + str(column) + '</th>'
    d += '</tr></thead><tbody>'

    for i in range(df.shape[0]):

        # Check if formatting is same for all columns in this row
        style_row = formatting_df.iloc[i,]
        if style_row.eq(style_row[0]).all():
            row_style = style_row[0]
            d += '\n<tr style="' + row_style + '">'
        else:
            d += '\n<tr>'
            row_style = None
        d += '<td>' + str(df.index[i]) + '</td>'    

        for j in range(df.shape[1]):
            
            if row_style is None:
                style = formatting_df.iloc[i,j]
                td = '<td style="' + style + '">'
            else:
                td = '<td>'

            if j == df.shape[1]-1: # Last column
                d += ' ' + td+str(df.iloc[i,j])+'</td> \n </tr>'

            else:
                d += '\n ' + td+str(df.iloc[i,j])+'</td>'

    d += '</tbody></table>'
                
    return d


def get_formatted_html_table(table_name, df):
    try:
        func_name = "sysproduction.reporting.table_formatting." + table_formatters[table_name]
        formatting_func = resolve_function(func_name)
    except:
        formatting_func = striped_table
    new_df, formatting_df = formatting_func(df)
    #return df.style.apply(formatting_func,axis=None).render()
    return create_html_table(new_df, formatting_df)


def striped_table(df):
    colors = {
        'striped1' : 'white',
        'striped2' : 'lightgrey',
    }
    x = df.copy()
    for y in range(len(df.index)):
        if y % 2 == 0:
            x.iloc[y,:] = 'background-color: ' + colors['striped1']
        else:
            x.iloc[y,:] = 'background-color: ' + colors['striped2']
    return df, x


def optimal_positions_table_formatting(df):

    colors = {
        'shorter':'crimson',
        'short':'pink',
        'long':'lightgreen',
        'longer':'limegreen',
        'closed':'cyan',
        'untaken_risk' : 'khaki',
    }

    abbreviations = { 
        "optimal_position" : "opt_pos",
        "position_limit_contracts" : "pos_limit_c",
        "previous_position" : "prev_pos",
        "weight_per_contract" : "w_per_c",
        "position_limit_weight" : "pos_limit_w",
        "optimum_weight" : "opt_w",
        "maximum_weight" : "max_w",
        "minimum_weight" : "min_w",
        "previous_weight" : "prev_w",
        "optimised_weight" : "optz_w",
        "optimised_position" : "optz_pos",
        "start_weight" : "start_w",
    }
    df = df.rename(columns=abbreviations)

    df, x = striped_table(df)

    short = df['optz_pos'] < 0
    shorter = (df['optz_pos'] < 0) & (df['optz_pos'] < df['prev_pos'])
    long = df['optz_pos'] > 0
    longer = (df['optz_pos'] > 0) & (df['optz_pos'] > df['prev_pos'])
    closed = (df['optz_pos'] == 0) & (df['prev_pos'] != 0)
    untaken_risk = (abs(df['opt_pos']) > 0.5) & (df['optz_pos'] == 0)

    x.loc[untaken_risk, :] = 'background-color: '+ colors['untaken_risk']
    x.loc[short, :] = 'background-color: '+ colors['short']
    x.loc[shorter, :] = 'background-color: '+ colors['shorter']
    x.loc[long, :] = 'background-color: '+ colors['long']
    x.loc[longer, :] = 'background-color: '+ colors['longer']
    x.loc[closed, :] = 'background-color: '+ colors['closed']

    return df, x


def roll_report_table_formatting(df):
    colors = {
        'alert' : 'red',
        'warning' : 'yellow'
    }
    df, x = striped_table(df)

    warning = (df['roll_expiry'] < 10) | (df['carry_expiry'] < 10)
    alert = (df['price_expiry'] < 5) 

    x.loc[warning, :] = 'background-color: '+ colors['warning']
    x.loc[alert, :] = 'background-color: '+ colors['alert']

    return df, x
