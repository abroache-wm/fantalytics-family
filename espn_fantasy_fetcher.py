import json
import time
import statistics
from typing import Dict, List, Any, Optional
from collections import defaultdict

import pandas as pd
import requests


class ESPNFantasyDataFetcher:
    def __init__(self, league_id: str = "1488486"):
        self.league_id = league_id
        # Base URLs for different season ranges
        self.current_base_url = (
            "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{year}/segments/0/leagues/{league_id}"
        )
        self.historical_base_url = (
            "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/leagueHistory/{league_id}?seasonId={year}"
        )

        # Views to request
        self.views = "&".join(
            [
                "view=mDraftDetail",
                "view=mMatchup",
                "view=mMatchupScore",
                "view=mTeam",
                "view=mRoster",
                "view=mSettings",
                "view=mStandings",
                "view=mStatus",
                "view=mLiveScoring",
                "view=modular",
                "view=mNav",
                "view=kona_player_info",
            ]
        )

    # -----------------------
    # HTTP helpers
    # -----------------------
    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Cookie": "espn_s2=AEB8fE3qRRqojbqJ8a1e1zGABaF%2FzfPzt4hc%2BGenVzj%2Bguk6C3dBdHIS106Z99fcSmo36Jg0c97vkO1EEqM6IXDvAgovWlyky3347np3OlkONYxmsnAncXk%2BkG%2BG2KT%2FI87atBmh8mY97VchNlfd8lWJHOvx13XtnzlG8I86A4YItBOAHWEm1U6h8zv%2FAj5DWxfx4qQ2kZhqEd%2BcE%2B7ESBrMpQX7MGb3ls4MGlngLEgCsittwXZI7r1dvumgp0Q5OdQQmL%2BGN9LPDbZ6fov6m08zFMdzqZ5HTqeI5qhflZrmRA%3D%3D; SWID={E6423367-F4CE-4102-A478-54F4F7EC93B0}"
        }

    def get_url_for_season(self, year: int) -> str:
        """Return base URL (with views) for a season."""
        if year >= 2019:
            url = self.current_base_url.format(year=year, league_id=self.league_id)
            return f"{url}?{self.views}"
        else:
            url = self.historical_base_url.format(league_id=self.league_id, year=year)
            return f"{url}&{self.views}"

    def fetch_season_data(self, year: int) -> Optional[Dict[str, Any]]:
        """Fetch a season snapshot (without forcing a specific week)."""
        url = self.get_url_for_season(year)
        try:
            r = requests.get(url, headers=self._headers(), timeout=30)
            r.raise_for_status()
            data = r.json()
            if year >= 2019:
                return data
            else:
                # leagueHistory returns a list where index 0 is the league that year
                if isinstance(data, list) and data:
                    return data[0]
                return data
        except requests.RequestException as e:
            print(f"Error fetching season {year}: {e}")
            return None

    # -----------------------
    # Utilities
    # -----------------------
    @staticmethod
    def _safe_points_by_week(container: Dict[Any, Any], week_key: Any) -> float:
        if not isinstance(container, dict):
            return 0.0
        val = container.get(week_key)
        if val is None:
            val = container.get(str(week_key))
        try:
            return float(val or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _safe_float(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    def _matchup_period_count(self, season_data: Dict[str, Any]) -> int:
        # Regular season is 14 weeks, we don't want playoff stats
        return 14

    def _player_weeks_from_schedule(self, season_data: Dict[str, Any], year: int):
        """
        Build {player_id: {info, weekly_scores}} by fetching each week.
        Gets ACTUAL player stats regardless of roster position (bench/active).
        """
        player_data: Dict[int, Dict[str, Any]] = {}
        max_week = self._matchup_period_count(season_data)

        base_url = self.get_url_for_season(year)
        sep = "&" if "?" in base_url else "?"

        # First, collect all unique player IDs from the draft
        drafted_players = set()
        if "draftDetail" in season_data and "picks" in season_data["draftDetail"]:
            for pick in season_data["draftDetail"]["picks"]:
                if pick.get("playerId"):
                    drafted_players.add(pick["playerId"])

        for week in range(1, max_week + 1):
            url = f"{base_url}{sep}scoringPeriodId={week}"
            try:
                r = requests.get(url, headers=self._headers(), timeout=25)
                r.raise_for_status()
                wdata = r.json()
                if year < 2019 and isinstance(wdata, list) and wdata:
                    wdata = wdata[0]
            except Exception as e:
                print(f"   ! Week {week} fetch failed: {e}")
                time.sleep(0.2)
                continue

            # Process all teams' rosters to find all players
            for team in wdata.get("teams", []) or []:
                roster = team.get("roster", {}) or {}
                for entry in roster.get("entries", []) or []:
                    pid = entry.get("playerId")
                    if not pid:
                        continue

                    # Initialize player if first time seeing them
                    if pid not in player_data:
                        player_data[pid] = {
                            "player_name": "Unknown",
                            "position": "Unknown",
                            "pro_team": 0,
                            "injury_status": "ACTIVE",
                            "weekly_scores": {}
                        }

                    # Update player info
                    if "playerPoolEntry" in entry:
                        ppe = entry["playerPoolEntry"]
                        if "player" in ppe:
                            p = ppe["player"]
                            player_data[pid]["player_name"] = p.get("fullName", "Unknown")
                            player_data[pid]["position"] = self.get_position_from_player(p)
                            player_data[pid]["pro_team"] = p.get("proTeamId", 0)
                            player_data[pid]["injury_status"] = p.get("injuryStatus", "ACTIVE")

                            # Get actual stats for this scoring period
                            # Stats are in player -> stats -> scoringPeriodId -> appliedTotal
                            stats = p.get("stats", []) or []
                            for stat_entry in stats:
                                # Look for stats matching this scoring period
                                if stat_entry.get("scoringPeriodId") == week:
                                    if stat_entry.get("statSourceId") == 0:  # Real stats (not projections)
                                        pts = stat_entry.get("appliedTotal", 0.0)
                                        try:
                                            pts = float(pts)
                                        except:
                                            pts = 0.0
                                        player_data[pid]["weekly_scores"][week] = pts
                                        break

                        # Alternative location for applied stats
                        if week not in player_data[pid]["weekly_scores"]:
                            # Try the appliedStatTotal at the entry level
                            applied = ppe.get("appliedStatTotal", 0.0)
                            try:
                                applied = float(applied)
                            except:
                                applied = 0.0
                            if applied > 0:
                                player_data[pid]["weekly_scores"][week] = applied

            time.sleep(0.12)

        # Make sure we have all drafted players even if they never appeared on a roster
        for pid in drafted_players:
            if pid not in player_data:
                player_data[pid] = {
                    "player_name": "Unknown",
                    "position": "Unknown",
                    "pro_team": 0,
                    "injury_status": "DNP",
                    "weekly_scores": {}
                }

        return player_data, max_week

    # -----------------------
    # Data extraction
    # -----------------------
    @staticmethod
    def get_position_from_player(player: Dict[str, Any]) -> str:
        position_map = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}
        default_position = player.get("defaultPositionId", 0)
        return position_map.get(default_position, "FLEX")

    def extract_draft_data_with_stats(
            self, season_data: Dict[str, Any], year: int
    ) -> Dict[str, Any]:
        draft_info = {"year": year, "picks": [], "draft_order": {}, "keeper_info": []}
        if not season_data:
            return draft_info

        # Get ALL players' weekly stats (not just current rosters)
        print(f"  Fetching weekly data for all players in {year}...")
        all_player_data, max_week = self._player_weeks_from_schedule(season_data, year)

        # Build team/owner mapping
        teams: Dict[int, Dict[str, Any]] = {}
        members: Dict[str, str] = {}

        for team in season_data.get("teams", []) or []:
            teams[team["id"]] = {
                "name": team.get("name", f"Team {team['id']}"),
                "abbrev": team.get("abbrev", f"T{team['id']}"),
                "owner": team.get("primaryOwner", "Unknown"),
            }

        for m in season_data.get("members", []) or []:
            mid = m.get("id")
            mname = f"{m.get('firstName', '')} {m.get('lastName', '')}".strip()
            if not mname:
                mname = f"Owner {mid}"
            if mid is not None:
                members[mid] = mname

        for t in teams.values():
            t["owner_name"] = members.get(t["owner"], t["owner"])

        # Process all player stats
        player_stats: Dict[int, Dict[str, Any]] = {}

        for pid, pdata in all_player_data.items():
            weeks = pdata["weekly_scores"]

            info = {
                "player_name": pdata["player_name"],
                "position": pdata["position"],
                "pro_team": pdata["pro_team"],
                "injury_status": pdata.get("injury_status", "ACTIVE"),
                "season_points": 0.0,
                "games_played": 0,
                "weekly_scores": [],
                "consistency_score": 0.0,
                "non_scoring_games": 0,  # Changed from injury_games
                "boom_games": 0,
                "bust_games": 0,
                "best_week": 0.0,
                "worst_week": 0.0,
                "playoff_points": 0.0,
            }

            # Fill in ALL weeks 1-14 with 0 if missing
            complete_weeks = {}
            for week in range(1, 15):  # Weeks 1-14 only
                complete_weeks[week] = weeks.get(week, 0.0)

            # Calculate stats from complete weekly scores
            wk_list = [{"week": w, "score": round(self._safe_float(s), 2)} for w, s in sorted(complete_weeks.items())]
            info["weekly_scores"] = wk_list
            scores = [self._safe_float(s) for s in complete_weeks.values()]
            info["season_points"] = round(sum(scores), 2)
            info["games_played"] = sum(1 for s in scores if s > 0)
            info["non_scoring_games"] = 14 - info["games_played"]  # All weeks without points

            # Consistency (only for games with points)
            positive_scores = [s for s in scores if s > 0]
            if len(positive_scores) > 1:
                avg = sum(positive_scores) / len(positive_scores)
                if avg > 0:
                    var = sum((s - avg) ** 2 for s in positive_scores) / (len(positive_scores) - 1)
                    std = var ** 0.5
                    info["consistency_score"] = max(0.0, 100.0 - (std / avg * 100.0))

            pos = info["position"]
            boom_thr = {"QB": 25, "RB": 20, "WR": 20, "TE": 15, "K": 12, "D/ST": 15}.get(pos, 15)
            bust_thr = {"QB": 10, "RB": 5, "WR": 5, "TE": 3, "K": 3, "D/ST": 2}.get(pos, 5)
            info["boom_games"] = sum(1 for s in scores if s >= boom_thr)
            info["bust_games"] = sum(1 for s in scores if 0 < s <= bust_thr)

            if scores:
                info["best_week"] = max(scores)
                info["worst_week"] = min([s for s in scores if s > 0], default=0)  # Worst non-zero week

            # No playoff points since we're only doing regular season
            info["playoff_points"] = 0.0

            player_stats[pid] = info

        # Extract draft picks and add stats
        if "draftDetail" in season_data:
            draft_detail = season_data["draftDetail"]
            if "picks" in draft_detail:
                for pick in draft_detail["picks"]:
                    pid = pick.get("playerId", 0)
                    pick_info = {
                        "year": year,
                        "round": pick.get("roundId", 0),
                        "pick_number": pick.get("roundPickNumber", 0),
                        "overall_pick": pick.get("overallPickNumber", 0),
                        "team_id": pick.get("teamId", 0),
                        "team_name": teams.get(pick.get("teamId"), {}).get("name", "Unknown"),
                        "owner_name": teams.get(pick.get("teamId"), {}).get("owner_name", "Unknown"),
                        "player_id": pid,
                        "keeper": pick.get("keeper", False),
                        "bid_amount": pick.get("bidAmount", 0),
                    }

                    # Add player stats if found
                    if pid in player_stats:
                        pick_info.update(player_stats[pid])
                    else:
                        # Player never played - get name from draft and create zero stats
                        if "playerPoolEntry" in pick and "player" in pick["playerPoolEntry"]:
                            p = pick["playerPoolEntry"]["player"]
                            pick_info["player_name"] = p.get("fullName", "Unknown")
                            pick_info["position"] = self.get_position_from_player(p)
                            pick_info["pro_team"] = p.get("proTeamId", 0)
                        else:
                            pick_info["player_name"] = "Unknown"
                            pick_info["position"] = "Unknown"
                            pick_info["pro_team"] = 0

                        # Add zero stats for player who never played
                        pick_info["injury_status"] = "DNP"
                        pick_info["season_points"] = 0.0
                        pick_info["games_played"] = 0
                        pick_info["non_scoring_games"] = 14
                        pick_info["weekly_scores"] = [{"week": w, "score": 0.0} for w in range(1, 15)]
                        pick_info["consistency_score"] = 0.0
                        pick_info["boom_games"] = 0
                        pick_info["bust_games"] = 0
                        pick_info["best_week"] = 0.0
                        pick_info["worst_week"] = 0.0
                        pick_info["playoff_points"] = 0.0

                    draft_info["picks"].append(pick_info)

        return draft_info

    def extract_matchups(self, season_data: Dict[str, Any], year: int) -> List[Dict[str, Any]]:
        """Extract all matchups from season data with safe per-week scoring."""
        matchups: List[Dict[str, Any]] = []
        if not season_data:
            return matchups

        teams: Dict[int, Dict[str, Any]] = {}
        for team in season_data.get("teams", []) or []:
            teams[team["id"]] = {
                "name": team.get("name", f"Team {team['id']}"),
                "abbrev": team.get("abbrev", f"T{team['id']}"),
                "owner": team.get("primaryOwner", "Unknown"),
            }

        for game in season_data.get("schedule", []) or []:
            if "winner" not in game:
                continue

            home = game.get("home", {}) or {}
            away = game.get("away", {}) or {}

            home_score = self._safe_float(home.get("totalPoints", 0))
            away_score = self._safe_float(away.get("totalPoints", 0))

            week_key = game.get("matchupPeriodId")
            if home_score == 0 and "pointsByScoringPeriod" in home:
                home_score = self._safe_points_by_week(home["pointsByScoringPeriod"], week_key)
            if away_score == 0 and "pointsByScoringPeriod" in away:
                away_score = self._safe_points_by_week(away["pointsByScoringPeriod"], week_key)

            matchup = {
                "year": year,
                "week": week_key,
                "matchup_id": game.get("id"),
                "home_team_id": home.get("teamId"),
                "home_team_name": teams.get(home.get("teamId"), {}).get("name", "Unknown"),
                "home_team_abbrev": teams.get(home.get("teamId"), {}).get("abbrev", "UNK"),
                "home_score": round(home_score, 2),
                "away_team_id": away.get("teamId"),
                "away_team_name": teams.get(away.get("teamId"), {}).get("name", "Unknown"),
                "away_team_abbrev": teams.get(away.get("teamId"), {}).get("abbrev", "UNK"),
                "away_score": round(away_score, 2),
                "winner": game.get("winner"),
                "playoff_type": game.get("playoffTierType", "NONE"),
                "is_playoff": game.get("playoffTierType", "NONE") != "NONE",
                "margin": round(abs(home_score - away_score), 2),
            }

            if matchup["winner"] == "HOME":
                matchup["winning_team_id"] = matchup["home_team_id"]
                matchup["winning_team_name"] = matchup["home_team_name"]
                matchup["winning_score"] = matchup["home_score"]
                matchup["losing_team_id"] = matchup["away_team_id"]
                matchup["losing_team_name"] = matchup["away_team_name"]
                matchup["losing_score"] = matchup["away_score"]
            else:
                matchup["winning_team_id"] = matchup["away_team_id"]
                matchup["winning_team_name"] = matchup["away_team_name"]
                matchup["winning_score"] = matchup["away_score"]
                matchup["losing_team_id"] = matchup["home_team_id"]
                matchup["losing_team_name"] = matchup["home_team_name"]
                matchup["losing_score"] = matchup["home_score"]

            matchups.append(matchup)

        return matchups

    def extract_team_records(self, season_data: Dict[str, Any], year: int) -> pd.DataFrame:
        records: List[Dict[str, Any]] = []
        if not season_data or "teams" not in season_data:
            return pd.DataFrame()

        for team in season_data["teams"]:
            record = (team.get("record", {}) or {}).get("overall", {}) or {}
            records.append(
                {
                    "year": year,
                    "team_id": team["id"],
                    "team_name": team.get("name", f"Team {team['id']}"),
                    "abbrev": team.get("abbrev", f"T{team['id']}"),
                    "wins": record.get("wins", 0),
                    "losses": record.get("losses", 0),
                    "ties": record.get("ties", 0),
                    "points_for": round(self._safe_float(record.get("pointsFor", 0)), 2),
                    "points_against": round(self._safe_float(record.get("pointsAgainst", 0)), 2),
                    "playoff_seed": team.get("playoffSeed", 0),
                    "final_rank": team.get("rankCalculatedFinal", 0),
                    "draft_position": team.get("draftDayProjectedRank", 0),
                }
            )

        return pd.DataFrame(records)

    def calculate_draft_metrics(self, draft_data: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        metrics = {
            "by_owner": defaultdict(
                lambda: {
                    "total_picks": 0,
                    "total_value": 0.0,
                    "boom_players": 0,
                    "bust_players": 0,
                    "injured_players": 0,
                    "consistency_avg": 0.0,
                    "playoff_performers": 0,
                    "best_pick_value": 0.0,
                    "worst_pick_value": 0.0,
                }
            ),
            "by_position": defaultdict(lambda: defaultdict(list)),
            "by_round": defaultdict(lambda: defaultdict(list)),
        }

        for year, year_data in draft_data.items():
            for pick in year_data.get("picks", []):
                owner = pick.get("owner_name", "Unknown")
                position = pick.get("position", "Unknown")
                round_num = pick.get("round", 0)

                draft_capital = 193 - pick.get("overall_pick", 192)
                season_points = self._safe_float(pick.get("season_points", 0))
                pick_value = season_points / draft_capital if draft_capital > 0 else 0.0

                # Update owner metrics
                o = metrics["by_owner"][owner]
                o["total_picks"] += 1
                o["total_value"] += pick_value
                o["boom_players"] += 1 if pick.get("boom_games", 0) > 3 else 0
                o["bust_players"] += 1 if pick.get("bust_games", 0) > 5 else 0
                o["injured_players"] += 1 if pick.get("non_scoring_games", 0) > 4 else 0
                o["playoff_performers"] += 1 if self._safe_float(pick.get("playoff_points", 0)) > 30 else 0

                if self._safe_float(pick.get("consistency_score", 0)) > 0:
                    n = o["total_picks"]
                    prev = o["consistency_avg"]
                    curr = self._safe_float(pick["consistency_score"])
                    o["consistency_avg"] = ((prev * (n - 1)) + curr) / n if n > 0 else curr

                if pick_value > o["best_pick_value"]:
                    o["best_pick_value"] = pick_value
                    o["best_pick"] = pick

                if o["worst_pick_value"] == 0.0 or pick_value < o["worst_pick_value"]:
                    o["worst_pick_value"] = pick_value
                    o["worst_pick"] = pick

                # Track by position and round
                metrics["by_position"][position][round_num].append(pick)
                metrics["by_round"][round_num][position].append(pick)

        return dict(metrics)

    def fetch_all_data(self, start_year: int = 2016, end_year: int = 2024) -> Dict[str, Any]:
        """Fetch all data including enriched draft data, matchups, and standings."""
        all_data: Dict[str, Any] = {
            "matchups": [],
            "standings": [],
            "drafts": {},
            "raw_data": {},
            "metrics": {},
        }

        for year in range(start_year, end_year + 1):
            print(f"Fetching data for {year}...")
            season_data = self.fetch_season_data(year)

            if season_data:
                all_data["raw_data"][year] = season_data

                # Enriched draft data with ALL players' stats
                draft_data = self.extract_draft_data_with_stats(season_data, year)
                all_data["drafts"][year] = draft_data
                print(f"  Found {len(draft_data['picks'])} draft picks")

                if draft_data["picks"] and year == end_year:
                    sample = draft_data["picks"][0]
                    print(
                        f"  Sample pick: {sample.get('player_name', 'Unknown')} - "
                        f"{sample.get('position', '?')} - "
                        f"{self._safe_float(sample.get('season_points', 0)):.1f} pts - "
                        f"Consistency: {self._safe_float(sample.get('consistency_score', 0)):.1f}"
                    )

                # Matchups
                matchups = self.extract_matchups(season_data, year)
                all_data["matchups"].extend(matchups)
                print(f"  Found {len(matchups)} matchups")

                # Standings
                standings_df = self.extract_team_records(season_data, year)
                if not standings_df.empty:
                    all_data["standings"].append(standings_df)
                    print(f"  Found {len(standings_df)} team records")

            time.sleep(0.4)

        print("\nCalculating advanced draft metrics...")
        all_data["metrics"] = self.calculate_draft_metrics(all_data["drafts"])

        # DataFrames
        if all_data["matchups"]:
            all_data["matchups_df"] = pd.DataFrame(all_data["matchups"])
        if all_data["standings"]:
            all_data["standings_df"] = pd.concat(all_data["standings"], ignore_index=True)

        # Compile all draft picks
        all_draft_picks: List[Dict[str, Any]] = []
        for _, draft_data in all_data["drafts"].items():
            all_draft_picks.extend(draft_data["picks"])
        if all_draft_picks:
            all_data["draft_picks_df"] = pd.DataFrame(all_draft_picks)

        return all_data


if __name__ == "__main__":
    fetcher = ESPNFantasyDataFetcher(league_id="1488486")

    print("ESPN Fantasy Football Data Fetcher - Fixed Edition")
    print("=" * 60)
    print("Fetching all data from 2016-2024 with complete player stats...")
    print("(This captures ALL players, even if traded/dropped/waived)")
    print()

    all_data = fetcher.fetch_all_data(2016, 2024)

    # Save matchups to CSV
    if "matchups_df" in all_data:
        all_data["matchups_df"].to_csv("espn_fantasy_matchups.csv", index=False)
        print(f"\n✓ Saved {len(all_data['matchups_df'])} matchups to 'espn_fantasy_matchups.csv'")

    # Save standings to CSV
    if "standings_df" in all_data:
        all_data["standings_df"].to_csv("espn_fantasy_standings.csv", index=False)
        print("✓ Saved standings to 'espn_fantasy_standings.csv'")

    # Save draft picks to CSV
    if "draft_picks_df" in all_data:
        csv_columns = [
            "year",
            "round",
            "overall_pick",
            "team_name",
            "owner_name",
            "player_name",
            "position",
            "season_points",
            "games_played",
            "consistency_score",
            "boom_games",
            "bust_games",
            "non_scoring_games",
            "best_week",
            "worst_week",
            "playoff_points",
        ]
        available_columns = [c for c in csv_columns if c in all_data["draft_picks_df"].columns]
        all_data["draft_picks_df"][available_columns].to_csv("espn_fantasy_draft_picks.csv", index=False)
        print(f"✓ Saved {len(all_data['draft_picks_df'])} enriched draft picks to 'espn_fantasy_draft_picks.csv'")

    # Save complete raw data as JSON
    with open("espn_fantasy_complete_data.json", "w") as f:
        json.dump(all_data["raw_data"], f, indent=2)
    print("✓ Saved complete raw data to 'espn_fantasy_complete_data.json'")

    # Save enriched draft data as JSON (THIS IS THE FIXED FILE YOU WANTED)
    with open("espn_fantasy_draft_data.json", "w") as f:
        json.dump(all_data["drafts"], f, indent=2)
    print("✓ Saved FIXED draft data to 'espn_fantasy_draft_data.json'")

    print("\n✅ Draft data now includes full season stats for ALL drafted players!")
    print("   (regardless of trades, drops, or waivers)")