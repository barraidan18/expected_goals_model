import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

class NHLDataPipeline:
    def __init__(self):
        self.base_url = "https://api-web.nhle.com/v1"
        self.shots_data = []
    
    def get_schedule(self, start_date, end_date):
        """Fetch NHL game schedule between two dates"""
        url = f"{self.base_url}/schedule/{start_date}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching schedule: {str(e)}")
            return {'dates': []}
    
    def get_game_data(self, game_id):
        """Fetch play-by-play data for a specific game"""
        url = f"{self.base_url}/gamecenter/{game_id}/play-by-play"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching game data: {str(e)}")
            return {'plays': []}
    
    def extract_shot_data(self, play_data, game_id):
        """Extract relevant shot information from play data"""
        shot_info = {
            'game_id': game_id,
            'event_idx': play_data.get('about', {}).get('eventIdx'),
            'period': play_data.get('about', {}).get('period'),
            'period_time': play_data.get('about', {}).get('periodTime'),
            'coordinates_x': play_data.get('coordinates', {}).get('x'),
            'coordinates_y': play_data.get('coordinates', {}).get('y'),
            'shot_type': play_data.get('result', {}).get('secondaryType'),
            'shooter': play_data.get('players', [{}])[0].get('player', {}).get('fullName'),
            'team': play_data.get('team', {}).get('name'),
            'event_type': play_data.get('result', {}).get('event'),
            'is_goal': play_data.get('result', {}).get('event') == 'Goal'
        }
        return shot_info
    
    def process_game(self, game_id):
        """Process all shots from a single game"""
        game_data = self.get_game_data(game_id)
        all_plays = game_data.get('liveData', {}).get('plays', {}).get('allPlays', [])
        
        shot_events = []
        for play in all_plays:
            event_type = play.get('result', {}).get('event')
            if event_type in ['Shot', 'Goal', 'Missed Shot', 'Blocked Shot']:
                shot_events.append(self.extract_shot_data(play, game_id))
        
        return shot_events
    
    def collect_data(self, start_date, end_date):
        """Collect shot data for all games in the date range"""
        schedule = self.get_schedule(start_date, end_date)
        
        for date in schedule.get('dates', []):
            for game in date.get('games', []):
                game_id = game['gamePk']
                print(f"Processing game {game_id}")
                
                try:
                    shot_events = self.process_game(game_id)
                    self.shots_data.extend(shot_events)
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    print(f"Error processing game {game_id}: {str(e)}")
    
    def save_to_csv(self, filename):
        """Save collected shot data to CSV in the flat_files directory"""
        import os
        
        # Define the project root directory path
        project_root = '/workspaces/expected_goals_model'
        
        # Create flat_files directory if it doesn't exist
        flat_files_dir = os.path.join(project_root, 'flat_files')
        os.makedirs(flat_files_dir, exist_ok=True)
        
        # Construct the full file path
        file_path = os.path.join(flat_files_dir, filename)
        
        # Save the data
        df = pd.DataFrame(self.shots_data)
        df.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")

# Example usage
if __name__ == "__main__":
    pipeline = NHLDataPipeline()
    
    # Collect data for a specific date range
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    pipeline.collect_data(start_date, end_date)
    pipeline.save_to_csv("nhl_shots_data.csv")  # Will be saved in ./flat_files/nhl_shots_data.csv