import pandas as pd
from imdb import IMDb
import time

# Initialize IMDb instance
ia = IMDb()

# Initialize an empty list to store movie data and a set for unique IDs
movies_data = {
    (1900, 1920): [],
    (1920, 1940): [],
    (1940, 1960): [],
    (1960, 1980): [],
    (1980, 2000): [],
    (2000, 2020): []
}

# Define the time periods and total records to collect
time_periods = [
    (1900, 1920),
    (1920, 1940),
    (1940, 1960),
    (1960, 1980),
    (1980, 2000),
    (2000, 2020)
]
total_records = 10  # Total records across all periods
records_per_period = total_records // len(time_periods)  # Equal records for each period

# Function to fetch and format data for a movie
def get_movie_data(movie):
    try:
        ia.update(movie, info=['main'])

        return {
            "tconst": movie.movieID,
            "titleType": movie.get('kind', ''),
            "primaryTitle": movie.get('title', ''),
            "startYear": movie.get('year', ''),
            "runtimeMinutes": movie.get('runtimes', [''])[0] if movie.get('runtimes') else '',
            "averageRating": movie.get('rating', ''),
            "numVotes": movie.get('votes', '')
        }

    except Exception as e:
        print(f"Error fetching data for {movie}: {e}")
        return None


search_index = 1
total_records_collected = 0  # Track total records collected across all periods

while total_records_collected < total_records:  # Stop when total records are collected
    movies = ia.search_movie(f"movie {search_index}")
    print(f"Search index: {search_index}, Found {len(movies)} movies")

    for movie in movies:
        data = get_movie_data(movie)

        if data and data['startYear']:
            year = int(data['startYear'])
            print(f"Processing {data['primaryTitle']} ({year})")

            # Distribute the movies into the correct time period
            if 1900 <= year < 1920 and len(movies_data[(1900, 1920)]) < records_per_period:
                movies_data[(1900, 1920)].append(data)
                total_records_collected += 1

            elif 1920 <= year < 1940 and len(movies_data[(1920, 1940)]) < records_per_period:
                movies_data[(1920, 1940)].append(data)
                total_records_collected += 1

            elif 1940 <= year < 1960 and len(movies_data[(1940, 1960)]) < records_per_period:
                movies_data[(1940, 1960)].append(data)
                total_records_collected += 1

            elif 1960 <= year < 1980 and len(movies_data[(1960, 1980)]) < records_per_period:
                movies_data[(1960, 1980)].append(data)
                total_records_collected += 1

            elif 1980 <= year < 2000 and len(movies_data[(1980, 2000)]) < records_per_period:
                movies_data[(1980, 2000)].append(data)
                total_records_collected += 1

            elif 2000 <= year < 2020 and len(movies_data[(2000, 2020)]) < records_per_period:
                movies_data[(2000, 2020)].append(data)
                total_records_collected += 1

            # If we've collected the target number of records, break the loop
            if total_records_collected >= total_records:
                break


    search_index += 1  # Increment search term to vary results for broader search

# Save each time period's data to separate CSV files
for period, data in movies_data.items():
    if data:  # Save only non-empty datasets
        df = pd.DataFrame(data)
        file_name = f"imdb_data_{period[0]}_{period[1]}.csv"
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}")
