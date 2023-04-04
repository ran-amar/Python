import pandas as pd


class CacheManager:
    def __init__(self, data: set[str]):
        self.data = {}
        self.total_games_count = 0
        self.parse_data(data)

    def parse_data(self, data: set[str]) -> None:
        res = {}
        for item in data:
            if item.endswith('.png'):
                try:
                    components = item.split('/')
                    sport_type = components[0]
                    frame_file = components[-1]
                    game_id = components[-3]

                    if sport_type not in res:
                        res[sport_type] = {}
                        res[sport_type]['games'] = {}
                    if game_id not in res[sport_type]['games']:
                        res[sport_type]['games'][game_id] = []
                        if 'games_counter' not in res[sport_type]:
                            res[sport_type]['games_counter'] = 0
                        res[sport_type]['games_counter'] += 1
                        self.total_games_count += 1
                    res[sport_type]['games'][game_id].append(frame_file)
                except Exception as e:
                    print(item, e)
        self.data = res

    def get_total_games_count(self) -> int:
        return self.total_games_count

    def get_games_frames(self, sport_type: str, num_games: int, frames_per_game: int):
        res = {}
        if sport_type not in self.data or num_games <= 0 or frames_per_game <= 0:
            raise Exception('Invalid parameters')

        games_idx = 0
        for game, frames in self.data[sport_type]['games'].items():
            if games_idx == num_games:
                break
            res[game] = []
            frames_idx = 0
            while frames_idx < frames_per_game and frames_idx < len(frames):
                res[game].append(frames[frames_idx])
                frames_idx += 1
            games_idx += 1

        return res

    def get_total_games_count_blacklist(self, black_list: list[str]) -> int:
        total_games_count = 0
        for sport_type in self.data:
            if sport_type not in black_list:
                total_games_count += self.data[sport_type]['games_counter']
        return total_games_count


if __name__ == '__main__':
    # Read the parquet file into a DataFrame
    df = pd.read_parquet('inventory_lsports-dev_full_14_03_2023_sample1M.parquet')

    cache_manager = CacheManager(set(df.key.to_list()))
    print(cache_manager.get_total_games_count())
    print(cache_manager.get_games_frames('AmericanFootball', 2, 1))
    print(cache_manager.get_total_games_count_blacklist(['AmericanFootball']))
