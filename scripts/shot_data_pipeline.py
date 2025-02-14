import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

class NHLDataPipeline:
    def __init__(self):
        self.base_url = "https://api-web.nhle.com/v1"
        self.plays_data = []  # Renamed from shots_data to plays_data
    
    def get_schedule(self, start_date, end_date):
        """Fetch NHL game schedule between two dates"""
        url = f"{self.base_url}/schedule/{start_date}"
        try:
            print(f"Fetching schedule from: {url}")
            response = requests.get(url)
            response.raise_for_status()
            schedule_data = response.json()
            
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
    
    def extract_play_data(self, play_data, game_data):
        """Extract relevant information from play data for all event types"""
        play_info = {
            'game_id': game_data['id'],
            'event_id': play_data.get('eventId'),
            'event_type': play_data.get('typeDescKey'),
            
            # Period information from periodDescriptor
            'period': play_data.get('periodDescriptor', {}).get('number'),
            'period_type': play_data.get('periodDescriptor', {}).get('periodType'),
            
            # Timing information
            'time_in_period': play_data.get('timeInPeriod'),
            'time_remaining': play_data.get('timeRemaining'),
            
            # Coordinates and zone (if available in details)
            'coordinates_x': play_data.get('details', {}).get('xCoord'),
            'coordinates_y': play_data.get('details', {}).get('yCoord'),
            'zone_code': play_data.get('details', {}).get('zoneCode'),
            
            # Team information
            'event_owner_team_id': play_data.get('details', {}).get('eventOwnerTeamId'),
            'away_team_id': game_data['awayTeam']['id'],
            'home_team_id': game_data['homeTeam']['id'],
            
            # Game situation
            'situation_code': play_data.get('situationCode'),
            'home_team_defending_side': play_data.get('homeTeamDefendingSide'),
        }
        
        # Add shot-specific details if available
        details = play_data.get('details', {})
        if play_data.get('typeDescKey') in ['shot-on-goal', 'goal', 'missed-shot', 'blocked-shot']:
            play_info.update({
                'shot_type': details.get('shotType'),
                'shooter_id': details.get('shootingPlayerId'),
                'goalie_id': details.get('goalieInNetId'),
                'away_sog': details.get('awaySOG'),
                'home_sog': details.get('homeSOG')
            })
            
            # Add goal-specific details
            if play_data.get('typeDescKey') == 'goal':
                play_info.update({
                    'scoring_player_id': details.get('scoringPlayerId'),
                    'assist1_player_id': details.get('assist1PlayerId'),
                    'assist2_player_id': details.get('assist2PlayerId'),
                    'away_score': details.get('awayScore'),
                    'home_score': details.get('homeScore')
                })
        
        return play_info
    
    def process_game(self, game_id, debug=False):
        """Process all plays from a single game"""
        game_data = self.get_game_data(game_id)
        plays = game_data.get('plays', [])
        
        if debug and plays:
            # Print the first play event's full data structure
            print("\nSample play data structure:")
            print(json.dumps(plays[0], indent=2))
        
        play_events = []
        for play in plays:
            play_events.append(self.extract_play_data(play, game_data))
        
        return play_events
    
    def collect_data(self, start_date, end_date, debug=False):
        """Collect play-by-play data for all games in the date range"""
        schedule = self.get_schedule(start_date, end_date)
        
        for game in schedule.get('games', []):
            game_id = game['id']
            away_team = game['awayTeam']['placeName']['default']
            home_team = game['homeTeam']['placeName']['default']
            print(f"\nProcessing game {game_id}: {away_team} @ {home_team}")
            
            try:
                play_events = self.process_game(game_id, debug)
                self.plays_data.extend(play_events)
                if debug:
                    # Exit after processing one game in debug mode
                    break
                time.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"Error processing game {game_id}: {str(e)}")
                
        if debug:
            print("\nFirst processed play data:")
            if self.plays_data:
                print(json.dumps(self.plays_data[0], indent=2))
    
    def save_to_csv(self, filename):
        """Save collected play data to CSV in the flat_files directory"""
        import os
        
        # Define the project root directory path
        project_root = '/workspaces/expected_goals_model'
        
        # Create flat_files directory if it doesn't exist
        flat_files_dir = os.path.join(project_root, 'flat_files')
        os.makedirs(flat_files_dir, exist_ok=True)
        
        # Construct the full file path
        file_path = os.path.join(flat_files_dir, filename)
        
        # Save the data
        df = pd.DataFrame(self.plays_data)
        df.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")

# Example usage
if __name__ == "__main__":
    pipeline = NHLDataPipeline()
    
    # Collect data for a specific date range
    start_date = "2025-02-09"
    end_date = "2024-01-31"
    
    pipeline.collect_data(start_date, end_date)
    pipeline.save_to_csv("nhl_plays_data.csv")  # Will be saved in ./flat_files/nhl_plays_data.csv