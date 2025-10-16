import pandas as pd
from flask import Flask, request, render_template, make_response
import io

app = Flask(__name__)

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
                # Use a stream to read the uploaded file to avoid saving it to disk
                stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
                df = pd.read_csv(stream)

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
                    # 1 J/cm^2 = 10000 J/m^2. 1 hour = 3600 seconds. W = J/s.
                    df['shortWaveRadiation'] = (df['shortWaveRadiation'] * 10000) / 3600
                else:
                    return render_template('index.html', error='CSV must contain a radiation column.')

                # Convert date column to datetime objects
                df['date_utc'] = pd.to_datetime(df['date_utc'])

                processed_stations = []
                # Group by station and process each one individually
                for station_code, station_df in df.groupby('stationCode'):
                    # Create a copy to avoid SettingWithCopyWarning
                    station_data = station_df.copy()
                    station_data.set_index('date_utc', inplace=True)

                    # Resample and interpolate
                    resampled_data = station_data[['shortWaveRadiation']].resample('15T').interpolate(method='linear')

                    # Round the values
                    resampled_data['shortWaveRadiation'] = resampled_data['shortWaveRadiation'].round().astype(int)

                    # Forward-fill station-specific information
                    resampled_data['stationCode'] = station_code
                    resampled_data['name'] = station_data['name'].iloc[0]
                    resampled_data.ffill(inplace=True)

                    processed_stations.append(resampled_data)

                # Combine all processed stations into a single DataFrame
                df_resampled = pd.concat(processed_stations)

                # Rename the column back to the original name
                df_resampled.rename(columns={'shortWaveRadiation': 'shortWaveRadiation [watts per square meter]'}, inplace=True)

                # Reorder columns to match the input format
                df_resampled = df_resampled[['stationCode', 'name', 'shortWaveRadiation [watts per square meter]']]
                
                # Reset index to make 'date_utc' a column again
                df_resampled.reset_index(inplace=True)
                df_resampled.rename(columns={'date_utc': 'date [UTC]'}, inplace=True)

                # Create a string buffer to hold the CSV data
                output = io.StringIO()
                df_resampled.to_csv(output, index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
                output.seek(0)

                # Create a response to send the file to the user
                response = make_response(output.getvalue())
                response.headers['Content-Disposition'] = 'attachment; filename=interpolated_weather_data.csv'
                response.headers['Content-type'] = 'text/csv'

                return response

            except Exception as e:
                return render_template('index.html', error=f'Error processing file: {e}')

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
