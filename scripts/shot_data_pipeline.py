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
            print(f"Fetching schedule from: {url}")
            response = requests.get(url)
            response.raise_for_status()
            schedule_data = response.json()
            
            # Find the specific game week containing our date
            all_games = []
            for week in schedule_data.get('gameWeek', []):
                if week.get('date') == start_date:
                    games = week.get('games', [])
                    print(f"\nFound {len(games)} games for {start_date}:")
                    for game in games:
                        print(f"Game ID: {game['id']} - {game['awayTeam']['placeName']['default']} vs {game['homeTeam']['placeName']['default']}")
                    all_games.extend(games)
                    break
            
            return {'games': all_games}
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching schedule: {str(e)}")
            return {'games': []}
    
    def get_game_data(self, game_id):
        """Fetch play-by-play data for a specific game"""
        url = f"{self.base_url}/gamecenter/{game_id}/play-by-play"
        try:
            print(f"Fetching game data from: {url}")
            response = requests.get(url)
            response.raise_for_status()
            game_data = response.json()
            print(f"Found {len(game_data.get('plays', []))} plays in game")
            return game_data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching game data: {str(e)}")
            return {'plays': []}
    
    def extract_shot_data(self, play_data, game_id):
        """Extract relevant shot information from play data"""
        shot_info = {
            'game_id': game_id,
            'event_index': play_data.get('eventId'),
            'period': play_data.get('period', {}).get('periodNumber'),
            'period_time': play_data.get('timeInPeriod'),
            'coordinates_x': play_data.get('details', {}).get('xCoord'),
            'coordinates_y': play_data.get('details', {}).get('yCoord'),
            'shot_type': play_data.get('details', {}).get('shotType'),
            'shooter': play_data.get('details', {}).get('shootingPlayerId'),
            'shooter_name': play_data.get('details', {}).get('shooterName'),
            'team': play_data.get('team', {}).get('abbrev'),
            'event_type': play_data.get('typeCode'),
            'is_goal': play_data.get('typeCode') == 'GOAL',
            # Game state information
            'home_score': play_data.get('homeScore'),
            'away_score': play_data.get('awayScore'),
            'home_team': play_data.get('homeTeam', {}).get('abbrev'),
            'away_team': play_data.get('awayTeam', {}).get('abbrev'),
            'strength': play_data.get('situationCode'),  # e.g., 'EV', 'PP', 'SH'
            'empty_net': play_data.get('details', {}).get('isEmptyNet'),
            'game_time_remaining': play_data.get('timeRemaining')
        }
        return shot_info
        return shot_info
    
    def process_game(self, game_id, debug=False):
        """Process all shots from a single game"""
        game_data = self.get_game_data(game_id)
        plays = game_data.get('plays', [])
        
        if debug and plays:
            # Print the first shot/goal event's full data structure
            for play in plays:
                if play.get('typeDescKey') in ['shot', 'goal', 'missed-shot', 'blocked-shot']:
                    print("\nSample play data structure:")
                    print(json.dumps(play, indent=2))
                    break
        
        shot_events = []
        for play in plays:
            event_type = play.get('typeDescKey')
            if event_type in ['shot', 'goal', 'missed-shot', 'blocked-shot']:
                shot_events.append(self.extract_shot_data(play, game_id))
        
        return shot_events
    
    def collect_data(self, start_date, end_date, debug=False):
        """Collect shot data for all games in the date range"""
        schedule = self.get_schedule(start_date, end_date)
        
        for game in schedule.get('games', []):
            game_id = game['id']
            away_team = game['awayTeam']['placeName']['default']
            home_team = game['homeTeam']['placeName']['default']
            print(f"\nProcessing game {game_id}: {away_team} @ {home_team}")
            
            try:
                shot_events = self.process_game(game_id, debug)
                self.shots_data.extend(shot_events)
                if debug:
                    # Exit after processing one game in debug mode
                    break
                time.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"Error processing game {game_id}: {str(e)}")
                
        if debug:
            print("\nFirst processed shot data:")
            if self.shots_data:
                print(json.dumps(self.shots_data[0], indent=2))
    
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
    start_date = "2025-02-09"
    end_date = "2024-01-31"
    
    pipeline.collect_data(start_date, end_date)
    pipeline.save_to_csv("nhl_shots_data.csv")  # Will be saved in ./flat_files/nhl_shots_data.csv