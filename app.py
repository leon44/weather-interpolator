import pandas as pd
from flask import Flask, request, render_template, make_response
import io
import os

app = Flask(__name__)

def process_interpolation(df):
    # Check for input format and rename columns
    if 'shortWaveRadiation [watts per square meter]' in df.columns:
        df.rename(columns={
            'date [UTC]': 'date_utc',
            'stationCode': 'stationCode',
            'name': 'name',
            'shortWaveRadiation [watts per square meter]': 'shortWaveRadiation'
        }, inplace=True)
    elif 'globalRadiation60Min [joules per square centimeter]' in df.columns:
        df.rename(columns={
            'date [UTC]': 'date_utc',
            'stationCode': 'stationCode',
            'name': 'name',
            'globalRadiation60Min [joules per square centimeter]': 'shortWaveRadiation'
        }, inplace=True)
        # Convert from J/cm^2 (sum over hour) to W/m^2 (average)
        df['shortWaveRadiation'] = (df['shortWaveRadiation'] * 10000) / 3600
    else:
        raise ValueError('CSV must contain a radiation column.')

    # Convert date column to datetime objects
    df['date_utc'] = pd.to_datetime(df['date_utc'])

    processed_stations = []
    # Group by station and process each one individually
    for station_code, station_df in df.groupby('stationCode'):
        station_data = station_df.copy()
        station_data.set_index('date_utc', inplace=True)
        resampled_data = station_data[['shortWaveRadiation']].resample('15T').interpolate(method='linear')
        resampled_data['shortWaveRadiation'] = resampled_data['shortWaveRadiation'].round(1)
        resampled_data['stationCode'] = station_code
        resampled_data['name'] = station_data['name'].iloc[0]
        resampled_data.ffill(inplace=True)
        processed_stations.append(resampled_data)

    df_resampled = pd.concat(processed_stations)
    df_resampled.rename(columns={'shortWaveRadiation': 'shortWaveRadiation [watts per square meter]'}, inplace=True)
    df_resampled = df_resampled[['stationCode', 'name', 'shortWaveRadiation [watts per square meter]']]
    df_resampled.reset_index(inplace=True)
    df_resampled.rename(columns={'date_utc': 'date [UTC]'}, inplace=True)

    output = io.StringIO()
    df_resampled.to_csv(output, index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
    output.seek(0)

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=interpolated_weather_data.csv'
    response.headers['Content-type'] = 'text/csv'
    return response

def check_gaps(df, tolerance=6):
    df.rename(columns={'date [UTC]': 'date_utc', 'stationCode': 'stationCode'}, inplace=True, errors='ignore')
    df['date_utc'] = pd.to_datetime(df['date_utc'])
    gap_results = []

    for station_code, station_df in df.groupby('stationCode'):
        station_df = station_df.sort_values('date_utc')
        time_diffs = station_df['date_utc'].diff().dt.total_seconds() / 3600
        gaps = time_diffs[time_diffs > tolerance]

        if not gaps.empty:
            for index, gap_hours in gaps.items():
                gap_start_time = station_df['date_utc'].iloc[index - 1]
                gap_end_time = station_df['date_utc'].iloc[index]
                gap_results.append(f"Station {station_code} has a gap of {gap_hours:.1f} hours between {gap_start_time.strftime('%Y-%m-%d %H:%M')} and {gap_end_time.strftime('%Y-%m-%d %H:%M')}.")
        else:
            gap_results.append(f"Station {station_code} has no significant gaps.")

    return render_template('index.html', gap_results=gap_results)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template('index.html', error='No file part')
        
        file = request.files['file']

        if file.filename == '':
            return render_template('index.html', error='No selected file')

        if file and file.filename.endswith('.csv'):
            try:
                stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
                df = pd.read_csv(stream)
                tool = request.form.get('tool')

                if tool == 'interpolator':
                    return process_interpolation(df)
                elif tool == 'gap_checker':
                    tolerance = int(request.form.get('gap_tolerance', 6))
                    return check_gaps(df, tolerance=tolerance)
                else:
                    return render_template('index.html', error='Invalid tool selected.')

            except Exception as e:
                return render_template('index.html', error=f'Error processing file: {e}')

    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
