
# Strava Analysis Project

This project analyzes workout data, focusing on activities like running and cycling. It integrates multiple data sources, such as Strava data, and retrieves weather data for the location of individual Strava activities, storing it in SQLite databases for further analysis.

## Project Structure

```
.
├── database
│   ├── database.db              # Database
├── requirements.txt             # Python dependencies
├── src
│   ├── api                      # API clients for interacting with external services
│   │   ├── strava_api.py        # Client for interacting with Strava API
│   │   ├── weather_api.py       # Client for interacting with the OpenMeteo API
│   │   └── weather_client.py    # Client for fetching weather data
│   └── config.py                 # Loading API config(s)
│   └── constants.py             # Dictionaries for mapping etc. 
│   └── db.py                    # SQLite database connections and schema setup
│   └── queries.py               # Common queries for interacting with the databases
│   ├── models                   # Data models for activity, best_efforts, gear, splits, zones, and weather
│   │   ├── activity.py          # Model for Strava activities in general
│   │   ├── best_efforts.py      # Model for extracting and processing best efforts 
│   │   ├── gear.py              # Model for extracting and processing gear (shoes, bikes etc.)
│   │   ├── split.py             # Model for extracting and processing splits data 
│   │   ├── zones.py             # Model for extracting and processing zones data (heartrate, pace etc.)
│   │   ├── weather.py           # Model for extracting relevant data from the Strava data to pass to the Weather API. 
├── main.py                      
```

## Setup

### Prerequisites
- Python 3.12 or higher
- `pip` (Python package manager)
- A Strava account with API access

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/strava-analysis.git
   cd strava-analysis
   ```

2. Set up a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. (Optional) Set up the project in editable mode for development:
   ```bash
   pip install -e .
   ```

### Setting Up the `.env` File

Create a `.env` file in the root of the project and add the following content, replacing the values with your own Strava credentials:

```
STRAVA_CLIENT_ID=your_client_id
STRAVA_CLIENT_SECRET=your_client_secret
STRAVA_REFRESH_TOKEN=your_refresh_token
STRAVA_ATHLETE_ID=your_athlete_id
```

## Usage

### Running the Analysis

To run the main script, execute:

```bash
python main.py
```

## API Clients

The project includes clients for interacting with external services:

- **Strava Client**: Fetches activity data from Strava.
- **Weather Client**: Retrieves weather data for activities.

## Database

The project uses SQLite databases to store activity, gear, and weather data. You can explore the database schema and write custom queries using the `db.py` and `queries.py` modules.
