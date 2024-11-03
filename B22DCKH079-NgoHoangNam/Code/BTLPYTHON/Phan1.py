import requests
from bs4 import BeautifulSoup
import sys
import pandas as pd
import time
import csv

# Thiết lập mã hóa UTF-8 cho đầu ra
sys.stdout.reconfigure(encoding='utf-8')


# Chức năng trợ giúp để trích xuất dữ liệu  với giá trị mặc định
def extract_data(tmp_tr, stat):
    return tmp_tr.find('td', {'data-stat': stat}).get_text(strip=True) if tmp_tr.find('td',{'data-stat': stat}) else "N/a"


# Hàm xử lý dữ liệu cầu thủ
def Data_Processing_of_Footballer(tmp_tr, team_name, player_data_tmp, mp):
    # Lấy thông tin cầu thủ
    player_name = tmp_tr.find('th', {'data-stat': 'player'}).get_text(strip=True)
    player_national = tmp_tr.find('td', {'data-stat': 'nationality'}).find('a')['href'].split('/')[-1].replace(
        '-Football', ' ') if tmp_tr.find('td', {'data-stat': 'nationality'}).find('a') else "N/a"
    player_position = extract_data(tmp_tr, 'position')
    player_age = extract_data(tmp_tr, 'age')

    # Playing time
    player_games = extract_data(tmp_tr, 'games')
    player_games_starts = extract_data(tmp_tr, 'games_starts')
    player_minutes = extract_data(tmp_tr, 'minutes')

    # Performance
    player_goals_pens = extract_data(tmp_tr, 'goals_pens')
    player_pens_made = extract_data(tmp_tr, 'pens_made')
    player_assists = extract_data(tmp_tr, 'assists')
    player_cards_yellow = extract_data(tmp_tr, 'cards_yellow')
    player_cards_red = extract_data(tmp_tr, 'cards_red')

    # Expected
    player_xg = extract_data(tmp_tr, 'xg')
    player_npxg = extract_data(tmp_tr, 'npxg')
    player_xg_assist = extract_data(tmp_tr, 'xg_assist')

    # Progression
    player_progressive_carries = extract_data(tmp_tr, 'progressive_carries')
    player_progressive_passes = extract_data(tmp_tr, 'progressive_passes')
    player_progressive_passes_received = extract_data(tmp_tr, 'progressive_passes_received')

    # Per 90 minutes
    player_stats_per90 = [
        extract_data(tmp_tr, 'goals_per90'),
        extract_data(tmp_tr, 'assists_per90'),
        extract_data(tmp_tr, 'goals_assists_per90'),
        extract_data(tmp_tr, 'goals_pens_per90'),
        extract_data(tmp_tr, 'goals_assists_pens_per90'),
        extract_data(tmp_tr, 'xg_per90'),
        extract_data(tmp_tr, 'xg_assist_per90'),
        extract_data(tmp_tr, 'xg_xg_assist_per90'),
        extract_data(tmp_tr, 'npxg_per90'),
        extract_data(tmp_tr, 'npxg_xg_assist_per90')
    ]

    # Thêm thông tin cầu thủ vào danh sách
    tmp = [
              player_name, player_national, team_name, player_position, player_age,
              player_games, player_games_starts, player_minutes,
              player_goals_pens, player_pens_made, player_assists,
              player_cards_yellow, player_cards_red, player_xg,
              player_npxg, player_xg_assist,
              player_progressive_carries, player_progressive_passes,
              player_progressive_passes_received
          ] + player_stats_per90

    player_data_tmp.append(tmp)
    mp[player_name] = player_data_tmp[-1]

    return player_data_tmp
# Hàm xử lý dữ liệu thủ môn
def Data_Processing_of_Goalkeeper(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_GA = extract_data('gk_goals_against')
    player_GA90 = extract_data('gk_goals_against_per90')
    player_SoTA = extract_data('gk_shots_on_target_against')
    player_Saves = extract_data('gk_saves')
    player_SaveP = extract_data('gk_save_pct')
    player_W = extract_data('gk_wins')
    player_D = extract_data('gk_ties')
    player_L = extract_data('gk_losses')
    player_CS = extract_data('gk_clean_sheets')
    player_CSP = extract_data('gk_clean_sheets_pct')
    player_PKatt = extract_data('gk_pens_att')
    player_PKA = extract_data('gk_pens_allowed')
    player_PKsv = extract_data('gk_pens_saved')
    player_PKm = extract_data('gk_pens_missed')
    player_SaveP = extract_data('gk_pens_save_pct')

    # Trả về list chỉ số thủ môn
    return [
        player_GA, player_GA90, player_SoTA, player_Saves, player_SaveP,
        player_W, player_D, player_L, player_CS, player_CSP,
        player_PKatt, player_PKA, player_PKsv, player_PKm, player_SaveP
    ]
# Hàm xử lý dữ liệu Shooting
def Data_Processing_of_Shooting(player):
    # Helper function to extract data with a default value
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    # Extracting shooting statistics using the helper function
    player_Gls = extract_data('goals')
    player_Sh = extract_data('shots')
    player_SoT = extract_data('shots_on_target')
    player_SoTP = extract_data('shots_on_target_pct')
    player_Sh90 = extract_data('shots_per90')
    player_Sot90 = extract_data('shots_on_target_per90')
    player_GSh = extract_data('goals_per_shot')
    player_GSoT = extract_data('goals_per_shot_on_target')
    player_Dist = extract_data('average_shot_distance')
    player_FK = extract_data('shots_free_kicks')
    player_PK = extract_data('pens_made')
    player_PKatt = extract_data('pens_att')
    player_xG = extract_data('xg')
    player_npxG = extract_data('npxg')
    player_npxGSh = extract_data('npxg_per_shot')
    player_GxG = extract_data('xg_net')
    player_npGxG = extract_data('npxg_net')

    # Trả về list chỉ số shooting
    return [
        player_Gls, player_Sh, player_SoT, player_SoTP, player_Sh90,
        player_Sot90, player_GSh, player_GSoT, player_Dist,
        player_FK, player_PK, player_PKatt, player_xG,
        player_npxG, player_npxGSh, player_GxG, player_npGxG
    ]
# Hàm xử lý dữ liệu Passing
def Data_Processing_of_Passing(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Cmp = extract_data('passes_completed')
    player_Att = extract_data('passes')
    player_CmpP = extract_data('passes_pct')
    player_TotDist = extract_data('passes_total_distance')
    player_PrgDist = extract_data('passes_progressive_distance')
    player_Short_Cmp = extract_data('passes_completed_short')
    player_Short_Att = extract_data('passes_short')
    player_Short_CmpP = extract_data('passes_pct_short')
    player_Medium_Cmp = extract_data('passes_completed_medium')
    player_Medium_Att = extract_data('passes_medium')
    player_Medium_CmpP = extract_data('passes_pct_medium')
    player_Long_Cmp = extract_data('passes_completed_long')
    player_Long_Att = extract_data('passes_long')
    player_Long_CmpP = extract_data('passes_pct_long')
    player_Ast = extract_data('assists')
    player_xAG = extract_data('xg_assist')
    player_xA = extract_data('pass_xa')
    player_xAG_net = extract_data('xg_assist_net')
    player_KP = extract_data('assisted_shots')
    player_1div3 = extract_data('passes_into_final_third')
    player_PPA = extract_data('passes_into_penalty_area')
    player_CrsPA = extract_data('crosses_into_penalty_area')
    player_PrgP = extract_data('progressive_passes')

    # Trả về list chỉ số passing
    return [
        player_Cmp, player_Att, player_CmpP, player_TotDist, player_PrgDist,
        player_Short_Cmp, player_Short_Att, player_Short_CmpP, player_Medium_Cmp,
        player_Medium_Att, player_Medium_CmpP, player_Long_Cmp, player_Long_Att,
        player_Long_CmpP, player_Ast, player_xAG, player_xA, player_xAG_net,
        player_KP, player_1div3, player_PPA, player_CrsPA, player_PrgP
    ]
# Hàm xử lý dữ liệu Pass Types
def Data_Processing_of_Pass_Types(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Live = extract_data('passes_live')
    player_Dead = extract_data('passes_dead')
    player_FK = extract_data('passes_free_kicks')
    player_TB = extract_data('through_balls')
    player_Sw = extract_data('passes_switches')
    player_Crs = extract_data('crosses')
    player_TI = extract_data('throw_ins')
    player_CK = extract_data('corner_kicks')
    player_In = extract_data('corner_kicks_in')
    player_Out = extract_data('corner_kicks_out')
    player_Str = extract_data('corner_kicks_straight')
    player_Cmp = extract_data('passes_completed')
    player_Off = extract_data('passes_offsides')
    player_Blocks = extract_data('passes_blocked')

    # Trả về list chỉ số pass types
    return [
        player_Live, player_Dead, player_FK, player_TB, player_Sw,
        player_Crs, player_TI, player_CK, player_In, player_Out,
        player_Str, player_Cmp, player_Off, player_Blocks
    ]


# Hàm xử lý dữ liệu Goal and Shot Creation
def Data_Processing_of_Goal_and_Shot_Creation(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {
            'data-stat': stat}) else "N/a"

    player_SCA = extract_data('sca')
    player_SCA90 = extract_data('sca_per90')
    player_SCA_PassLive = extract_data('sca_passes_live')
    player_SCA_PassDead = extract_data('sca_passes_dead')
    player_SCA_TO = extract_data('sca_take_ons')
    player_SCA_Sh = extract_data('sca_shots')
    player_SCA_Fld = extract_data('sca_fouled')
    player_SCA_Def = extract_data('sca_defense')

    player_GCA = extract_data('gca')
    player_GCA90 = extract_data('gca_per90')
    player_GCA_PassLive = extract_data('gca_passes_live')
    player_GCA_PassDead = extract_data('gca_passes_dead')
    player_GCA_TO = extract_data('gca_take_ons')
    player_GCA_Sh = extract_data('gca_shots')
    player_GCA_Fld = extract_data('gca_fouled')
    player_GCA_Def = extract_data('gca_defense')

    # Trả về list chỉ số Goal and Shot Creation
    return [
        player_SCA, player_SCA90, player_SCA_PassLive, player_SCA_PassDead,
        player_SCA_TO, player_SCA_Sh, player_SCA_Fld, player_SCA_Def,
        player_GCA, player_GCA90, player_GCA_PassLive, player_GCA_PassDead,
        player_GCA_TO, player_GCA_Sh, player_GCA_Fld, player_GCA_Def
    ]
# Hàm xử lý dữ liệu Defensive Actions
def Data_Processing_of_Defensive_Actions(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Tackes_Tkl = extract_data('tackles')
    player_Tackes_TklW = extract_data('tackles_won')
    player_Tackes_Def3rd = extract_data('tackles_def_3rd')
    player_Tackes_Mid3rd = extract_data('tackles_mid_3rd')
    player_Tackes_Att3rd = extract_data('tackles_att_3rd')
    player_Challenges_Tkl = extract_data('challenge_tackles')
    player_Challenges_Att = extract_data('challenges')
    player_Challenges_TklP = extract_data('challenge_tackles_pct')
    player_Challenges_Lost = extract_data('challenges_lost')
    player_Blocks = extract_data('blocks')
    player_Blocks_Sh = extract_data('blocked_shots')
    player_Blocks_Pass = extract_data('blocked_passes')
    player_Int = extract_data('interceptions')
    player_Tkl_Int = extract_data('tackles_interceptions')
    player_Clr = extract_data('clearances')
    player_Err = extract_data('errors')

    # Trả về list chỉ số Defensive Actions
    return [
        player_Tackes_Tkl, player_Tackes_TklW, player_Tackes_Def3rd,
        player_Tackes_Mid3rd, player_Tackes_Att3rd, player_Challenges_Tkl,
        player_Challenges_Att, player_Challenges_TklP, player_Challenges_Lost,
        player_Blocks, player_Blocks_Sh, player_Blocks_Pass,
        player_Int, player_Tkl_Int, player_Clr, player_Err
    ]
# Hàm xử lý dữ liệu Possession
def Data_Processing_of_Possession(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Touches = extract_data('touches')
    player_DefPen = extract_data('touches_def_pen_area')
    player_Def3rd = extract_data('touches_def_3rd')
    player_Mid3rd = extract_data('touches_mid_3rd')
    player_Att3rd = extract_data('touches_att_3rd')
    player_AttPen = extract_data('touches_att_pen_area')
    player_Touches_Live = extract_data('touches_live_ball')
    player_Att = extract_data('take_ons')
    player_Succ = extract_data('take_ons_won')
    player_SuccP = extract_data('take_ons_won_pct')
    player_Tkld = extract_data('take_ons_tackled')
    player_TkldP = extract_data('take_ons_tackled_pct')
    player_Carries = extract_data('carries')
    player_TotDist = extract_data('carries_distance')
    player_PrgDist = extract_data('carries_progressive_distance')
    player_PrgCarries = extract_data('progressive_carries')
    player_1div3 = extract_data('carries_into_final_third')
    player_CPA = extract_data('carries_into_penalty_area')
    player_Mis = extract_data('miscontrols')
    player_Dis = extract_data('dispossessed')
    player_Rec = extract_data('passes_received')
    player_PrgR = extract_data('progressive_passes_received')

    # Trả về list chỉ số Possession
    return [
        player_Touches, player_DefPen, player_Def3rd, player_Mid3rd,
        player_Att3rd, player_AttPen, player_Touches_Live, player_Att,
        player_Succ, player_SuccP, player_Tkld, player_TkldP, player_Carries,
        player_TotDist, player_PrgDist, player_PrgCarries, player_1div3,
        player_CPA, player_Mis, player_Dis, player_Rec, player_PrgR
    ]
# Hàm xử lý dữ liệu Playing Time
def Data_Processing_of_Playing_Time(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Starts = extract_data('games_starts')
    player_MinStart = extract_data('minutes_per_start')
    player_Compl = extract_data('games_complete')
    player_Subs = extract_data('games_subs')
    player_MnStart = extract_data('minutes_per_sub')
    player_unSub = extract_data('unused_subs')
    player_PPM = extract_data('points_per_game')
    player_onG = extract_data('on_goals_for')
    player_onGA = extract_data('on_goals_against')
    player_onxG = extract_data('on_xg_for')
    player_onxGA = extract_data('on_xg_against')

    # Trả về list chỉ số Playing Time
    return [
        player_Starts, player_MinStart, player_Compl, player_Subs,
        player_MnStart, player_unSub, player_PPM, player_onG,
        player_onGA, player_onxG, player_onxGA
    ]
# Hàm xử lý dữ liệu Miscellaneous Stats
def Data_Processing_of_Miscellaneous_Stats(player):
    def extract_data(stat):
        return player.find('td', {'data-stat': stat}).get_text(strip=True) if player.find('td', {'data-stat': stat}) else "N/a"

    player_Fls = extract_data('fouls')
    player_Fld = extract_data('fouled')
    player_Off = extract_data('offsides')
    player_Crs = extract_data('crosses')
    player_OG = extract_data('own_goals')
    player_Recov = extract_data('ball_recoveries')
    player_Won = extract_data('aerials_won')
    player_Lost = extract_data('aerials_lost')
    player_WonP = extract_data('aerials_won_pct')

    # Trả về list chỉ số Miscellaneous Stats
    return [
        player_Fls, player_Fld, player_Off, player_Crs,
        player_OG, player_Recov, player_Won, player_Lost,
        player_WonP
    ]


# Hàm cào dữ liệu lấy thông tin cầu thủ của từng đội bóng
def Crawl_Data_For_Each_Team(players_data, team_data):
    # Lấy thông tin và các chỉ số cầu thủ của mỗi đội
    for team in team_data:
        team_name = team[0]
        team_url = team[1]

        print(f"[][][]Đang cào dữ liệu cầu thủ của đội {team_name}..........[][][]")
        # Cào url của từng đội bóng
        r_tmp = requests.get(team_url)
        soup_tmp = BeautifulSoup(r_tmp.content, 'html.parser')

        # Danh sách tạm thời chứa thông tin tất cả cầu thủ của đội bóng hiện tại
        player_data_tmp = []
        mp = {}  # Map ánh xạ đến list chứa thông tin và chỉ số của cầu thủ thông qua key là tên cầu thủ

        # Tìm bảng chứa thông tin các cầu thủ
        player_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_standard_9'
        })
        if player_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = player_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                for player in players:
                    player_minutes_matches = player.find('td', {'data-stat': 'minutes'}).get_text(
                        strip=True) if player.find('td', {'data-stat': 'minutes'}).get_text(strip=True) else "N/a"
                    # Lọc ra những cầu thủ đã thi đấu ít nhất 90 phút
                    if player_minutes_matches == "N/a" or int(player_minutes_matches.replace(',', '')) < 90:
                        continue
                    player_data_tmp = Data_Processing_of_Footballer(player, team_name, player_data_tmp, mp)
            else:
                print(f"<Không tìm thấy thẻ <tbody> trong bảng cầu thủ>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa cầu thủ trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin các thủ môn
        Goalkeeper_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_keeper_9'
        })
        if Goalkeeper_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Goalkeeper_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các thủ môn
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Goalkeeper(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 15
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng thủ môn.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thủ môn trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Shooting của các cầu thủ
        Shooting_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_shooting_9'
        })
        if Shooting_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Shooting_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Shooting(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 17
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Shooting.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Shooting trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Passing của các cầu thủ
        Passing_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_passing_9'
        })
        if Passing_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Passing_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Passing(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 23
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Passing.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Passing trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Pass Types của các cầu thủ
        Pass_Types_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_passing_types_9'
        })
        if Pass_Types_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Pass_Types_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Pass_Types(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 14
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Pass Types.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Pass Types trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Goal and Shot Creation của các cầu thủ
        Goal_Shot_Creation_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_gca_9'
        })
        if Goal_Shot_Creation_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Goal_Shot_Creation_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Goal_and_Shot_Creation(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 16
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Goal and Shot Creation.>")
        else:
            print(
                f"<Không tìm thấy thẻ <table> chứa thông tin Goal and Shot Creation trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Defensive Actions của các cầu thủ
        Defensive_Actions_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_defense_9'
        })
        if Defensive_Actions_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Defensive_Actions_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Defensive_Actions(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 16
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Defensive Actions.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Defensive Actions trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Possession của các cầu thủ
        Possession_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_possession_9'
        })
        if Possession_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Possession_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Possession(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 22
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Possession.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Possession trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Playing Time của các cầu thủ
        Playing_Time_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_playing_time_9'
        })
        if Playing_Time_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Playing_Time_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Playing_Time(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 11
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Playing Time.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Playing Time trong trang của đội {team_name}.>")

        # Tìm bảng chứa thông tin Miscellaneous Stats của các cầu thủ
        Miscellaneous_Stats_table = soup_tmp.find('table', {
            'class': 'stats_table sortable min_width',
            'id': 'stats_misc_9'
        })
        if Miscellaneous_Stats_table:
            # Tìm thẻ <tbody> trong <table>
            tbody = Miscellaneous_Stats_table.find('tbody')
            if tbody:
                players = tbody.find_all('tr')
                # Danh sách lưu tạm thời tên các cầu thủ
                list_tmp = []

                for player in players:
                    player_name = player.find('th', {'data-stat': 'player'}).get_text(strip=True)
                    if player_name in mp:
                        mp[player_name] += Data_Processing_of_Miscellaneous_Stats(player)
                        list_tmp.append(player_name)

                for player in player_data_tmp:
                    if player[0] not in list_tmp:
                        player += ["N/a"] * 9
            else:
                print(f"<Không tìm thấy thẻ <tbody> bảng Miscellaneous Stats.>")
        else:
            print(f"<Không tìm thấy thẻ <table> chứa thông tin Miscellaneous Stats trong trang của đội {team_name}.>")

        # Thêm dữ liệu các cầu thủ của đội bóng vào danh sách chứa dữ liệu của tất cả các cầu thủ
        players_data += player_data_tmp
        print(f"<<<<<<<<Đã cào xong dữ liệu cầu thủ của đội {team_name}.>>>>>>>")

        # Tạm nghỉ trước khi cào đội tiếp theo
        time.sleep(10)

    return players_data


if __name__ == "__main__":
    url = 'https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')

    # Tìm bảng chứa thông tin các đội bóng trong mùa giải 2023-2024
    table = soup.find('table', {
        'class': 'stats_table sortable min_width force_mobilize',
        'id': 'results2023-202491_overall'
    })

    # Danh sách chứa dữ liệu đội bóng và URL
    team_data = []

    if table:
        # Tìm thẻ <tbody> trong <table>
        tbody = table.find('tbody')
        if tbody:
            # Lấy tất cả các thẻ <a> có định dạng như yêu cầu trong <tbody>
            teams = tbody.find_all('a', href=True)

            for team in teams:
                if "squads" in team['href']:
                    team_name = team.get_text(strip=True)
                    team_url = "https://fbref.com" + team['href']
                    team_data.append([team_name, team_url])

            print("-+-+-+-+Danh sách các đội bóng đã được lấy thành công.+-+-+-+-")
        else:
            print("Không tìm thấy kqua <tbody>.")
    else:
        print("Không tìm thấy thẻ <table>.")

    # #  Danh sach chứa từng cầu thủ của đội bóng
    players_data = []
    players_data = Crawl_Data_For_Each_Team(players_data, team_data)

    # Sắp xếp dữ liệu theo first name và tuổi giảm dần
    players_data = sorted(players_data, key=lambda x: (x[0].split()[0], -int(x[4])))

    # # Chuyển dữ liệu thành DataFrame và lưu thành file CSV
    df_players = pd.DataFrame(players_data,
                              columns=["Player Name", "Nation", "Team", "Position", "Age", "Matches Played", "Starts",
                                       "Minutes", "Non-Penalty Goals", "Penalties Made", "Assists", "Yellow Cards",
                                       "Red Cards", "xG", "npxG", "xAG", "PrgC", "PrgP", "PrgR", "Gls/90", "Ast/90",
                                       "G+A/90", "G-PK/90", "G+A-PK/90", "xG/90", "xAG/90", "xG+xAG/90", "npxG/90",
                                       "npxG+xAG/90",
                                       "Goalkeeping_GA", "GGoalkeeping_GA90", "Goalkeeping_SoTA", "Goalkeeping_Saves",
                                       "Goalkeeping_Save%", "Goalkeeping_W", "Goalkeeping_D", "Goalkeeping_L",
                                       "Goalkeeping_CS", "Goalkeeping_CS%", "Goalkeeping_PKatt", "Goalkeeping_PKA",
                                       "Goalkeeping_Pksv", "Goalkeeping_PKm", "Goalkeeping_Save%",
                                       "Shooting_Gls", "Shooting_Sh", "Shooting_SoT", "Shooting_SoT%", "Shooting_Sh/90",
                                       "Shooting_SoT/90", "Shooting_G/Sh", "Shooting_G/SoT", "Shooting_Dist",
                                       "Shooting_FK", "Shooting_PK", "Shooting_PKatt", "Shooting_xG", "Shooting_npxG",
                                       "Shooting_npxG/Sh", "Shooting_G-xG", "Shooting_np:G-xG",
                                       "Passing_Cmp", "Passing_Att", "Passing_Cmp%", "Passing_ToDist",
                                       "Passing_PrgDist", "Passing_Short_Cmp", "Passing_Short_Att",
                                       "Passing_Short_Cmp%", "Passing_Med_Cmp", "Passing_Med_Att", "Passing_Med_Cmp%",
                                       "Passing_Long_Cmp", "Passing_Long_Att", "Passing_Long_Cmp%", "Passing_Ast",
                                       "Passing_xAG", "Passing_xA", "Passing_A-xAG", "Passing_KP", "Passing_1/3",
                                       "Passing_PPA", "Passing_CrsPA", "Passing_PrgP",
                                       "Pass_Types_Live", "Pass_Types_Dead", "Pass_Types_FK", "Pass_Types_TB",
                                       "Pass_Types_Sw", "Pass_Types_Crs", "Pass_Types_TI", "Pass_Types_CK",
                                       "Pass_Types_In", "Pass_Types_Out", "Pass_Types_Str", "Pass_Types_Gmp",
                                       "Pass_Types_Off", "Pass_Types_Blocks",
                                       "GSCreation_SCA", "GSCreation_SCA90", "GGSCreation_SCA_PassLive",
                                       "GSCreation_SCA_PassDead", "GSCreation_SCA_TO", "GSCreation_SCA_Sh",
                                       "GSCreation_SCA_Fld", "GSCreation_SCA_Def", "GSCreation_GCA", "GSCreation_GCA90",
                                       "GSCreation_GCA_PassLive", "GSCreation_GCA_PassDead", "GSCreation_GCA_TO",
                                       "GSCreation_GCA_Sh", "GSCreation_GCA_Fld", "GSCreation_GCA_Def",
                                       "DActions_Tkl", "DActions_TklW", "DActions_Def3rd", "DActions_Mid3rd",
                                       "DActions_Att3rd", "DActions_Challenges_Tkl", "DActions_Challenges_Att",
                                       "DActions_Challenges_Tkl%", "DActions_Challenges_Lost", "DActions_Blocks",
                                       "DActions_Blocks_Sh", "DActions_Blocks_Pass", "DActions_Int", "DActions_Tkl+Int",
                                       "DActions_Clr", "DActions_Err",
                                       "Possession_Touches", "Possession_Def Pen", "Possession_Def 3rd",
                                       "Possession_Mid 3rd", "Possession_Att 3rd", "Possession_Att Pen",
                                       "Possession_Live", "Possession_Att", "Possession_Succ", "Possession_Succ%",
                                       "Possession_Tkld", "Possession_Tkld%", "Possession_Carries",
                                       "Possession_TotDist", "Possession_PrgDist", "Possession_PrgC", "Possession_1/3",
                                       "Possession_CPA", "Possession_Mis", "Possession_Dis", "Possession_Rec",
                                       "Possession_PrgR",
                                       "PTime_Starts", "PTime_Mn/Start", "PTime_Compl", "PTime_Subs", "PTime_Mn/Sub",
                                       "PTime_unSub", "PTime_PPM", "PTime_onG", "PTime_onGA", "PTime_onxG",
                                       "PTime_onxGA",
                                       "MStats_Fls", "MStats_Fld", "MStats_Off", "MStats_Crs", "MStats_OG",
                                       "MStats_Recov", "MStats_Won", "MStats_Lost", "MStats_Won%"
                                       ])
    df_players.to_csv("results.csv", index=False, encoding='utf-8-sig')
    print("<----------Đã lưu thông tin các cầu thủ vào file results.csv---------->")



