import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional
import time


class ESPNFantasyDataFetcher:
    def __init__(self, league_id: str = "690481"):
        self.league_id = league_id
        # Base URLs for different season ranges
        self.current_base_url = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{year}/segments/0/leagues/{league_id}"
        self.historical_base_url = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/leagueHistory/{league_id}?seasonId={year}"

        # Updated view parameters to include draft detail
        self.views = "&".join([
            "view=mDraftDetail",  # Critical for accurate draft data
            "view=mMatchup",
            "view=mMatchupScore",
            "view=mTeam",
            "view=mRoster",
            "view=mSettings",
            "view=mStandings",
            "view=mStatus",
            "view=mLiveScoring",
            "view=modular",
            "view=mNav"
        ])

    def get_url_for_season(self, year: int) -> str:
        """Get the appropriate URL based on the season year"""
        if year >= 2019:
            url = self.current_base_url.format(year=year, league_id=self.league_id)
            return f"{url}?{self.views}"
        else:
            url = self.historical_base_url.format(league_id=self.league_id, year=year)
            return f"{url}&{self.views}"

    def fetch_season_data(self, year: int) -> Optional[Dict[str, Any]]:
        """Fetch data for a specific season"""
        url = self.get_url_for_season(year)

        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            if year >= 2019:
                return response.json()
            else:
                data = response.json()
                return data[0] if isinstance(data, list) and data else data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {year}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text[:500]}")
            return None

    def extract_draft_data(self, season_data: Dict[str, Any], year: int) -> Dict[str, Any]:
        """Extract comprehensive draft data from season data"""
        draft_info = {
            'year': year,
            'picks': [],
            'draft_order': {},
            'keeper_info': []
        }

        if not season_data:
            return draft_info

        # Get team mapping
        teams = {}
        owner_mapping = {}
        if 'teams' in season_data:
            for team in season_data['teams']:
                teams[team['id']] = {
                    'name': team.get('name', f"Team {team['id']}"),
                    'abbrev': team.get('abbrev', f"T{team['id']}"),
                    'owner': team.get('primaryOwner', 'Unknown')
                }
                owner_mapping[team['id']] = team.get('primaryOwner', 'Unknown')

        # Get member names for owner mapping
        members = {}
        if 'members' in season_data:
            for member in season_data['members']:
                member_name = f"{member.get('firstName', '')} {member.get('lastName', '')}".strip()
                if not member_name:
                    member_name = f"Owner {member['id']}"
                members[member['id']] = member_name

        # Update team names with actual owner names
        for team_id, team_info in teams.items():
            if team_info['owner'] in members:
                team_info['owner_name'] = members[team_info['owner']]
            else:
                team_info['owner_name'] = team_info['owner']

        # Extract draft detail if available
        if 'draftDetail' in season_data:
            print(f"    Found draftDetail for {year}")
            draft_detail = season_data['draftDetail']

            # Get draft picks
            if 'picks' in draft_detail:
                print(f"    Processing {len(draft_detail['picks'])} draft picks")
                for pick in draft_detail['picks']:
                    pick_info = {
                        'year': year,
                        'round': pick.get('roundId', 0),
                        'pick_number': pick.get('roundPickNumber', 0),
                        'overall_pick': pick.get('overallPickNumber', 0),
                        'team_id': pick.get('teamId', 0),
                        'team_name': teams.get(pick.get('teamId'), {}).get('name', 'Unknown'),
                        'owner_name': teams.get(pick.get('teamId'), {}).get('owner_name', 'Unknown'),
                        'player_id': pick.get('playerId', 0),
                        'keeper': pick.get('keeper', False),
                        'bid_amount': pick.get('bidAmount', 0)  # For auction drafts
                    }

                    # Try to get player info from the pick
                    if 'playerPoolEntry' in pick:
                        player_entry = pick['playerPoolEntry']
                        if 'player' in player_entry:
                            player = player_entry['player']
                            pick_info['player_name'] = player.get('fullName', 'Unknown')
                            pick_info['position'] = self.get_position_from_player(player)
                            pick_info['pro_team'] = player.get('proTeamId', 0)

                    draft_info['picks'].append(pick_info)
            else:
                print(f"    No 'picks' found in draftDetail for {year}")

            # Get draft order
            if 'draftOrder' in draft_detail:
                draft_info['draft_order'] = draft_detail['draftOrder']

            # Get draft settings
            if 'draftSettings' in draft_detail:
                draft_info['draft_type'] = draft_detail['draftSettings'].get('type', 'SNAKE')
                draft_info['auction_budget'] = draft_detail['draftSettings'].get('auctionBudget', None)
        else:
            print(f"    No draftDetail found for {year}")

        # Alternative: Extract from roster if draftDetail is incomplete or missing
        if not draft_info['picks'] and 'teams' in season_data:
            print(f"    Attempting to extract draft data from rosters for {year}...")
            for team in season_data['teams']:
                if 'roster' in team and 'entries' in team['roster']:
                    for entry in team['roster']['entries']:
                        # Check if player was drafted (not waiver/free agent)
                        if entry.get('acquisitionType') == 'DRAFT':
                            pick_info = {
                                'year': year,
                                'team_id': team['id'],
                                'team_name': team.get('name', 'Unknown'),
                                'owner_name': teams.get(team['id'], {}).get('owner_name', 'Unknown'),
                                'player_id': entry.get('playerId', 0),
                                'acquisition_date': entry.get('acquisitionDate', 0),
                                'keeper': entry.get('keeperValue', 0) > 0
                            }

                            # Get player details
                            if 'playerPoolEntry' in entry:
                                player_entry = entry['playerPoolEntry']
                                if 'player' in player_entry:
                                    player = player_entry['player']
                                    pick_info['player_name'] = player.get('fullName', 'Unknown')
                                    pick_info['position'] = self.get_position_from_player(player)
                                    pick_info['pro_team'] = player.get('proTeamId', 0)

                                    # Get season stats if available
                                    if 'stats' in player:
                                        for stat in player['stats']:
                                            if stat.get('id') == f"00{year}" or stat.get('seasonId') == year:
                                                pick_info['season_points'] = stat.get('appliedTotal', 0)
                                                pick_info['season_average'] = stat.get('appliedAverage', 0)
                                                break

                            draft_info['picks'].append(pick_info)

            if draft_info['picks']:
                print(f"    Extracted {len(draft_info['picks'])} picks from rosters")

        return draft_info

    def get_position_from_player(self, player: Dict[str, Any]) -> str:
        """Extract position from player object"""
        position_map = {
            1: 'QB',
            2: 'RB',
            3: 'WR',
            4: 'TE',
            5: 'K',
            16: 'D/ST'
        }

        default_position = player.get('defaultPositionId', 0)
        return position_map.get(default_position, 'FLEX')

    def extract_matchups(self, season_data: Dict[str, Any], year: int) -> List[Dict[str, Any]]:
        """Extract all matchups from season data"""
        matchups = []

        if not season_data:
            return matchups

        # Get team mapping
        teams = {}
        if 'teams' in season_data:
            for team in season_data['teams']:
                teams[team['id']] = {
                    'name': team.get('name', f"Team {team['id']}"),
                    'abbrev': team.get('abbrev', f"T{team['id']}"),
                    'owner': team.get('primaryOwner', 'Unknown')
                }

        # Parse schedule/matchups
        schedule = season_data.get('schedule', [])
        for game in schedule:
            # Skip games that haven't been played
            if 'winner' not in game:
                continue

            home = game.get('home', {})
            away = game.get('away', {})

            # Extract scores
            home_score = home.get('totalPoints', 0)
            away_score = away.get('totalPoints', 0)

            # Alternative score extraction from pointsByScoringPeriod
            if home_score == 0 and 'pointsByScoringPeriod' in home:
                week_str = str(game.get('matchupPeriodId', 0))
                home_score = home['pointsByScoringPeriod'].get(week_str, 0)

            if away_score == 0 and 'pointsByScoringPeriod' in away:
                week_str = str(game.get('matchupPeriodId', 0))
                away_score = away['pointsByScoringPeriod'].get(week_str, 0)

            matchup = {
                'year': year,
                'week': game.get('matchupPeriodId'),
                'matchup_id': game.get('id'),
                'home_team_id': home.get('teamId'),
                'home_team_name': teams.get(home.get('teamId'), {}).get('name', 'Unknown'),
                'home_team_abbrev': teams.get(home.get('teamId'), {}).get('abbrev', 'UNK'),
                'home_score': round(home_score, 2),
                'away_team_id': away.get('teamId'),
                'away_team_name': teams.get(away.get('teamId'), {}).get('name', 'Unknown'),
                'away_team_abbrev': teams.get(away.get('teamId'), {}).get('abbrev', 'UNK'),
                'away_score': round(away_score, 2),
                'winner': game.get('winner'),
                'playoff_type': game.get('playoffTierType', 'NONE'),
                'is_playoff': game.get('playoffTierType', 'NONE') != 'NONE',
                'margin': round(abs(home_score - away_score), 2)
            }

            # Add winner info
            if matchup['winner'] == 'HOME':
                matchup['winning_team_id'] = matchup['home_team_id']
                matchup['winning_team_name'] = matchup['home_team_name']
                matchup['winning_score'] = matchup['home_score']
                matchup['losing_team_id'] = matchup['away_team_id']
                matchup['losing_team_name'] = matchup['away_team_name']
                matchup['losing_score'] = matchup['away_score']
            else:
                matchup['winning_team_id'] = matchup['away_team_id']
                matchup['winning_team_name'] = matchup['away_team_name']
                matchup['winning_score'] = matchup['away_score']
                matchup['losing_team_id'] = matchup['home_team_id']
                matchup['losing_team_name'] = matchup['home_team_name']
                matchup['losing_score'] = matchup['home_score']

            matchups.append(matchup)

        return matchups

    def extract_team_records(self, season_data: Dict[str, Any], year: int) -> pd.DataFrame:
        """Extract team records and standings"""
        records = []

        if not season_data or 'teams' not in season_data:
            return pd.DataFrame()

        for team in season_data['teams']:
            record = team.get('record', {}).get('overall', {})
            records.append({
                'year': year,
                'team_id': team['id'],
                'team_name': team.get('name', f"Team {team['id']}"),
                'abbrev': team.get('abbrev', f"T{team['id']}"),
                'wins': record.get('wins', 0),
                'losses': record.get('losses', 0),
                'ties': record.get('ties', 0),
                'points_for': round(record.get('pointsFor', 0), 2),
                'points_against': round(record.get('pointsAgainst', 0), 2),
                'playoff_seed': team.get('playoffSeed', 0),
                'final_rank': team.get('rankCalculatedFinal', 0),
                'draft_position': team.get('draftDayProjectedRank', 0)
            })

        return pd.DataFrame(records)

    def fetch_all_data(self, start_year: int = 2016, end_year: int = 2024) -> Dict[str, Any]:
        """Fetch all data including draft, matchups, and standings"""
        all_data = {
            'matchups': [],
            'standings': [],
            'drafts': {},
            'raw_data': {}
        }

        for year in range(start_year, end_year + 1):
            print(f"Fetching data for {year}...")
            season_data = self.fetch_season_data(year)

            if season_data:
                # Store raw data
                all_data['raw_data'][year] = season_data

                # Extract draft data
                draft_data = self.extract_draft_data(season_data, year)
                all_data['drafts'][year] = draft_data
                print(f"  Found {len(draft_data['picks'])} draft picks")

                # Extract matchups
                matchups = self.extract_matchups(season_data, year)
                all_data['matchups'].extend(matchups)
                print(f"  Found {len(matchups)} matchups")

                # Extract standings
                standings_df = self.extract_team_records(season_data, year)
                if not standings_df.empty:
                    all_data['standings'].append(standings_df)
                    print(f"  Found {len(standings_df)} team records")
            else:
                print(f"  No data found for {year}")

            time.sleep(0.5)  # Be nice to ESPN's servers

        # Convert to DataFrames
        if all_data['matchups']:
            all_data['matchups_df'] = pd.DataFrame(all_data['matchups'])

        if all_data['standings']:
            all_data['standings_df'] = pd.concat(all_data['standings'], ignore_index=True)

        # Compile all draft picks into a single DataFrame
        all_draft_picks = []
        for year, draft_data in all_data['drafts'].items():
            all_draft_picks.extend(draft_data['picks'])

        if all_draft_picks:
            all_data['draft_picks_df'] = pd.DataFrame(all_draft_picks)

        return all_data


# Usage example
if __name__ == "__main__":
    # Initialize fetcher
    fetcher = ESPNFantasyDataFetcher(league_id="690481")

    # Fetch all data from 2016-2024
    print("Fetching all data from 2016-2024...")
    all_data = fetcher.fetch_all_data(2016, 2024)

    # Save matchups to CSV
    if 'matchups_df' in all_data:
        all_data['matchups_df'].to_csv('espn_fantasy_matchups.csv', index=False)
        print(f"\nSaved {len(all_data['matchups_df'])} matchups to 'espn_fantasy_matchups.csv'")
        print("\nMatchups summary by year:")
        print(all_data['matchups_df'].groupby('year').size())

    # Save standings to CSV
    if 'standings_df' in all_data:
        all_data['standings_df'].to_csv('espn_fantasy_standings.csv', index=False)
        print(f"\nSaved standings to 'espn_fantasy_standings.csv'")
        print("\nTop 5 teams by total wins:")
        team_totals = all_data['standings_df'].groupby('team_name').agg({
            'wins': 'sum',
            'losses': 'sum',
            'points_for': 'sum'
        }).sort_values('wins', ascending=False)
        print(team_totals.head())

    # Save draft picks to CSV
    if 'draft_picks_df' in all_data:
        all_data['draft_picks_df'].to_csv('espn_fantasy_draft_picks.csv', index=False)
        print(f"\nSaved {len(all_data['draft_picks_df'])} draft picks to 'espn_fantasy_draft_picks.csv'")
        print("\nDraft picks summary by year:")
        print(all_data['draft_picks_df'].groupby('year').size())

        # Show sample of draft data to verify accuracy
        print("\nSample draft picks (first 10):")
        print("\nAvailable columns:", all_data['draft_picks_df'].columns.tolist())

        # Only show columns that actually exist
        display_cols = []
        for col in ['year', 'round', 'overall_pick', 'player_name', 'position', 'team_name', 'team_id', 'player_id']:
            if col in all_data['draft_picks_df'].columns:
                display_cols.append(col)

        if display_cols:
            print(all_data['draft_picks_df'].head(10)[display_cols])
        else:
            print(all_data['draft_picks_df'].head(10))

    # Save complete raw data as JSON
    print("\n\nSaving complete raw data...")
    with open('espn_fantasy_complete_data.json', 'w') as f:
        json.dump(all_data['raw_data'], f, indent=2)
    print(f"Saved complete data for {len(all_data['raw_data'])} seasons to 'espn_fantasy_complete_data.json'")

    # Save draft-specific data as JSON for easier analysis
    with open('espn_fantasy_draft_data.json', 'w') as f:
        json.dump(all_data['drafts'], f, indent=2)
    print(f"Saved draft data to 'espn_fantasy_draft_data.json'")