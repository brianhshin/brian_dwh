from riotwatcher import LolWatcher, ApiError
from datetime import timedelta
import pandas as pd
import re
import datetime as dt
import sys
import argparse

###############################################################################
def get_summoner_data(my_region, username, watcher):

    summoner = watcher.summoner.by_name(my_region, username)
    summoner_df = pd.DataFrame([summoner])
    return summoner_df

################################################################################
def get_champions_data(versions, watcher):

    champions = watcher.data_dragon.champions(versions['n']['champion'], False, 'en_US')['data']

    champions_df = pd.DataFrame(champions).transpose()
    champions_df.reset_index(inplace=True)
    champions_df.rename(columns={'index': 'champion'}, inplace=True)

    champions_stats_list = []
    for key in champions:
        champions[key]['stats']['champion'] = str(key)
        champions_stats_list.append(champions[key]['stats'])
    champions_stats_df = pd.DataFrame(champions_stats_list)

    champions_info_list = []
    for key in champions:
        champions[key]['info']['champion'] = str(key)
        champions_info_list.append(champions[key]['info'])
    champions_info_df = pd.DataFrame(champions_info_list)

    champions_df.drop(['info', 'stats', 'image'], 1, inplace=True)
    champions_complete_df = champions_df.merge(
        champions_stats_df, on='champion', how='left').merge(
            champions_info_df, on='champion', how='left')

    return champions_complete_df

####################################
def get_items_data(versions, watcher):

    items = watcher.data_dragon.items(versions['n']['item'], 'en_US')['data']
    items_df = pd.DataFrame(items).transpose()
    items_df.reset_index(inplace=True)
    items_df.rename(columns={'index': 'item_id'}, inplace=True)

    items_gold_list = []
    for key in items:
        items[key]['gold']['item_id'] = str(key)
        items_gold_list.append(items[key]['gold'])
    items_gold_df = pd.DataFrame(items_gold_list)

    items_df.drop(['colloq', 'image', 'gold', 'maps'], 1, inplace=True)
    clean = re.compile('<.*?>')
    items_df['description'] = items_df['description'].apply(lambda x: re.sub(clean, '', x))

    items_complete_df = items_df.merge(
        items_gold_df, on='item_id', how='left')

    return items_complete_df

####################################
def get_runes_data(versions, watcher):

    runes = watcher.data_dragon.runes(versions['n']['rune'], 'en_US')['data']
    runes_df = pd.DataFrame(runes).transpose()
    runes_df.reset_index(inplace=True)
    runes_df.rename(columns={'index': 'rune_id'}, inplace=True)

    runes_stats_list = []
    for key in runes:
        runes[key]['rune']['rune_id'] = str(key)
        runes_stats_list.append(runes[key]['rune'])
    runes_stats_df = pd.DataFrame(runes_stats_list)

    runes_df.drop(['image', 'rune'], 1, inplace=True)
    runes_complete_df = runes_df.merge(
        runes_stats_df, on='rune_id', how='left')
    return runes_complete_df

####################################
def get_runes_reforged_data(versions, watcher):

    runes_reforged = watcher.data_dragon.runes_reforged(versions['n']['rune'], 'en_US')
    runes_reforged_df = pd.DataFrame(runes_reforged)

    runes_reforged_slots_list = []
    for i in range(len(runes_reforged)):
        runes_reforged[i]['runes_reforged_id'] = runes_reforged[i].pop('id')

        for slot in runes_reforged[i]['slots']:
            for rune in slot['runes']:
                rune['runes_reforged_id'] = int(runes_reforged[i]['runes_reforged_id'])
                runes_reforged_slots_list.append(rune)
    runes_reforged_slots_df = pd.DataFrame(runes_reforged_slots_list)

    runes_reforged_df.rename(
        columns={
            'id': 'runes_reforged_id',
            'key': 'runes_reforged_key',
            'name': 'runes_reforged_name'},
        inplace=True)

    runes_reforged_slots_df.rename(
        columns={
            'id': 'rune_id',
            'key': 'rune_key',
            'name': 'rune_name'},
        inplace=True)

    runes_reforged_df.drop(['slots', 'icon'], 1, inplace=True)
    runes_reforged_slots_df.drop(['icon'], 1, inplace=True)
    runes_reforged_complete_df = runes_reforged_slots_df.merge(
        runes_reforged_df, on='runes_reforged_id', how='left')
    return runes_reforged_complete_df

################################################################################
def get_summoner_league_data(my_region, summoner_df, watcher):

    solo_league = watcher.league.by_summoner(my_region, summoner_df['id'][0])[0]
    flex_league = watcher.league.by_summoner(my_region, summoner_df['id'][0])[1]

    solo_league_df = pd.DataFrame([solo_league])
    flex_league_df = pd.DataFrame([flex_league])
    return solo_league_df, flex_league_df

###############################################################################
def get_summoner_matches_data(my_region, summoner_df, how_many_matches, watcher):

    summoner_ranked_stats = watcher.league.by_summoner(my_region, summoner_df['id'][0])
    summoner_matches = watcher.match.matchlist_by_account(my_region, summoner_df['accountId'][0])

    summoner_ranked_stats_df = pd.DataFrame(summoner_ranked_stats)
    summoner_matches_df = pd.DataFrame(summoner_matches['matches'])
    summoner_matches_df['summoner_id'] = summoner_df['id'][0]

    match_stats_df = pd.DataFrame()
    participants_identities_list = []

    print(f'pulling data from last {how_many_matches} matches.')

    for i in range(len(summoner_matches['matches'][:how_many_matches])):

        match = summoner_matches['matches'][i]
        print(f'pulling data from match {i+1}: {match}.')
        match_detail = watcher.match.by_id(my_region, match['gameId'])

        participants = []


        for row in match_detail['participants']:
            participants_row = {}
            participants_row['participantId'] = row['participantId']
            participants_row['teamId'] = row['teamId']
            participants_row['champion'] = row['championId']
            participants_row['spell1'] = row['spell1Id']
            participants_row['spell2'] = row['spell2Id']
            participants_row['role'] = row['timeline']['role']
            participants_row['lane'] = row['timeline']['lane']
            participants_row['win'] = row['stats']['win']
            participants_row['item0'] = row['stats']['item0']
            participants_row['item1'] = row['stats']['item1']
            participants_row['item2'] = row['stats']['item2']
            participants_row['item3'] = row['stats']['item3']
            participants_row['item4'] = row['stats']['item4']
            participants_row['item5'] = row['stats']['item5']
            participants_row['item6'] = row['stats']['item6']
            participants_row['kills'] = row['stats']['kills']
            participants_row['deaths'] = row['stats']['deaths']
            participants_row['assists'] = row['stats']['assists']
            participants_row['largestKillingSpree'] = row['stats']['largestKillingSpree']
            participants_row['largestMultiKill'] = row['stats']['largestMultiKill']
            participants_row['killingSprees'] = row['stats']['killingSprees']
            participants_row['longestTimeSpentLiving'] = row['stats']['longestTimeSpentLiving']
            participants_row['doubleKills'] = row['stats']['doubleKills']
            participants_row['tripleKills'] = row['stats']['tripleKills']
            participants_row['quadraKills'] = row['stats']['quadraKills']
            participants_row['pentaKills'] = row['stats']['pentaKills']
            participants_row['unrealKills'] = row['stats']['unrealKills']
            participants_row['totalDamageDealt'] = row['stats']['totalDamageDealt']
            participants_row['magicDamageDealt'] = row['stats']['magicDamageDealt']
            participants_row['physicalDamageDealt'] = row['stats']['physicalDamageDealt']
            participants_row['trueDamageDealt'] = row['stats']['trueDamageDealt']
            participants_row['largestCriticalStrike'] = row['stats']['largestCriticalStrike']
            participants_row['totalDamageDealtToChampions'] = row['stats']['totalDamageDealtToChampions']
            participants_row['magicDamageDealtToChampions'] = row['stats']['magicDamageDealtToChampions']
            participants_row['physicalDamageDealtToChampions'] = row['stats']['physicalDamageDealtToChampions']
            participants_row['trueDamageDealtToChampions'] = row['stats']['trueDamageDealtToChampions']
            participants_row['totalHeal'] = row['stats']['totalHeal']
            participants_row['totalUnitsHealed'] = row['stats']['totalUnitsHealed']
            participants_row['damageSelfMitigated'] = row['stats']['damageSelfMitigated']
            participants_row['damageDealtToObjectives'] = row['stats']['damageDealtToObjectives']
            participants_row['damageDealtToTurrets'] = row['stats']['damageDealtToTurrets']
            participants_row['visionScore'] = row['stats']['visionScore']
            participants_row['timeCCingOthers'] = row['stats']['timeCCingOthers']
            participants_row['totalDamageTaken'] = row['stats']['totalDamageTaken']
            participants_row['magicalDamageTaken'] = row['stats']['magicalDamageTaken']
            participants_row['physicalDamageTaken'] = row['stats']['physicalDamageTaken']
            participants_row['trueDamageTaken'] = row['stats']['trueDamageTaken']
            participants_row['goldEarned'] = row['stats']['goldEarned']
            participants_row['goldSpent'] = row['stats']['goldSpent']
            participants_row['turretKills'] = row['stats']['turretKills']
            participants_row['inhibitorKills'] = row['stats']['inhibitorKills']
            participants_row['totalMinionsKilled'] = row['stats']['totalMinionsKilled']
            participants_row['neutralMinionsKilled'] = row['stats']['neutralMinionsKilled']
            participants_row['neutralMinionsKilledTeamJungle'] = row['stats']['neutralMinionsKilledTeamJungle']
            participants_row['neutralMinionsKilledEnemyJungle'] = row['stats']['neutralMinionsKilledEnemyJungle']
            participants_row['totalTimeCrowdControlDealt'] = row['stats']['totalTimeCrowdControlDealt']
            participants_row['champLevel'] = row['stats']['champLevel']
            participants_row['visionWardsBoughtInGame'] = row['stats']['visionWardsBoughtInGame']
            participants_row['sightWardsBoughtInGame'] = row['stats']['sightWardsBoughtInGame']
            participants_row['wardsPlaced'] = row['stats']['wardsPlaced']
            participants_row['wardsKilled'] = row['stats']['wardsKilled']
            participants_row['firstBloodKill'] = row['stats']['firstBloodKill']
            participants_row['firstBloodAssist'] = row['stats']['firstBloodAssist']
            # participants_row['firstTowerKill'] = row['stats']['firstTowerKill']
            # participants_row['firstTowerAssist'] = row['stats']['firstTowerAssist']
            participants_row['firstInhibitorKill'] = row['stats']['firstInhibitorKill']
            participants_row['firstInhibitorAssist'] = row['stats']['firstInhibitorAssist']
            participants_row['combatPlayerScore'] = row['stats']['combatPlayerScore']
            participants_row['objectivePlayerScore'] = row['stats']['objectivePlayerScore']
            participants_row['totalPlayerScore'] = row['stats']['totalPlayerScore']
            participants_row['totalScoreRank'] = row['stats']['totalScoreRank']
            participants_row['playerScore0'] = row['stats']['playerScore0']
            participants_row['playerScore1'] = row['stats']['playerScore1']
            participants_row['playerScore2'] = row['stats']['playerScore2']
            participants_row['playerScore3'] = row['stats']['playerScore3']
            participants_row['playerScore4'] = row['stats']['playerScore4']
            participants_row['playerScore5'] = row['stats']['playerScore5']
            participants_row['playerScore6'] = row['stats']['playerScore6']
            participants_row['playerScore7'] = row['stats']['playerScore7']
            participants_row['playerScore8'] = row['stats']['playerScore8']
            participants_row['playerScore9'] = row['stats']['playerScore9']
            participants_row['perk0'] = row['stats']['perk0']
            participants_row['perk0Var1'] = row['stats']['perk0Var1']
            participants_row['perk0Var2'] = row['stats']['perk0Var2']
            participants_row['perk0Var3'] = row['stats']['perk0Var3']
            participants_row['perk1'] = row['stats']['perk1']
            participants_row['perk1Var1'] = row['stats']['perk1Var1']
            participants_row['perk1Var2'] = row['stats']['perk1Var2']
            participants_row['perk1Var3'] = row['stats']['perk1Var3']
            participants_row['perk2'] = row['stats']['perk2']
            participants_row['perk2Var1'] = row['stats']['perk2Var1']
            participants_row['perk2Var2'] = row['stats']['perk2Var2']
            participants_row['perk2Var3'] = row['stats']['perk2Var3']
            participants_row['perk3'] = row['stats']['perk3']
            participants_row['perk3Var1'] = row['stats']['perk3Var1']
            participants_row['perk3Var2'] = row['stats']['perk3Var2']
            participants_row['perk3Var3'] = row['stats']['perk3Var3']
            participants_row['perk4'] = row['stats']['perk4']
            participants_row['perk4Var1'] = row['stats']['perk4Var1']
            participants_row['perk4Var2'] = row['stats']['perk4Var2']
            participants_row['perk4Var3'] = row['stats']['perk4Var3']
            participants_row['perk5'] = row['stats']['perk5']
            participants_row['perk5Var1'] = row['stats']['perk5Var1']
            participants_row['perk5Var2'] = row['stats']['perk5Var2']
            participants_row['perk5Var3'] = row['stats']['perk5Var3']
            participants_row['perkPrimaryStyle'] = row['stats']['perkPrimaryStyle']
            participants_row['perkSubStyle'] = row['stats']['perkSubStyle']
            participants_row['statPerk0'] = row['stats']['statPerk0']
            participants_row['statPerk1'] = row['stats']['statPerk1']
            participants_row['statPerk2'] = row['stats']['statPerk2']


            participants.append(participants_row)

        df = pd.DataFrame(participants)
        df['gameId'] = match['gameId']
        match_stats_df = pd.concat([match_stats_df, df])

        for i in range(len(match_detail['participantIdentities'])):
            match_detail['participantIdentities'][i]['player']['participantId'] = float(match_detail['participantIdentities'][i]['participantId'])
            participants_identities_list.append(match_detail['participantIdentities'][i]['player'])

            match_detail['participantIdentities'][i]['player']['gameId'] = match_detail['gameId']

    participants_identities_df = pd.DataFrame(participants_identities_list)
    summoner_matches_df.reset_index(drop=True, inplace=True)
    match_stats_complete_df = match_stats_df.merge(
        participants_identities_df, on=['gameId', 'participantId'], how='left')

    return summoner_ranked_stats_df, summoner_matches_df, match_stats_complete_df

###############################################################################
def convert_column_datatypes(df, column_datatypes):

    for column in column_datatypes:
        df[column[0]] = df[column[0]].astype(column[1])
    return df

###############################################################################
def get_username_and_api_key():

    parser = argparse.ArgumentParser(description='league_scraper_local.py pulls league data from the api.')
    parser.add_argument('--username', default='rickyyytan', help='enter your league username. the default is me. rickyyytan.')
    parser.add_argument('--api_key', default='no key', help='enter the generated riot api key. there is no default.')
    parser.add_argument('--matches', default='10', help='enter how many matches you want to pull. default is 10 and 100 is max.')

    args = parser.parse_args()

    # are league usernames case sensitive?
    username = args.username
    api_key = args.api_key
    how_many_matches = int(args.matches)

    return username, api_key, how_many_matches

###############################################################################
def get_watcher_help():

    help(watcher)
    help(watcher.summoner)
    summoner = watcher.summoner.by_name(my_region, username)
    watcher.summoner.by_account(my_region, summoner['accountId'])
    watcher.summoner.by_id(my_region, summoner['id'])
    watcher.summoner.by_puuid(my_region, summoner['puuid'])
    help(watcher.champion)
    champion = watcher.champion.rotations(my_region)
    help(watcher.champion_mastery)
    watcher.champion_mastery.by_summoner(my_region, summoner['id'])
    watcher.champion_mastery.by_summoner_by_champion(my_region, summoner['id'], champion['freeChampionIds'][0])
    watcher.champion_mastery.scores_by_summoner(my_region, summoner['id'])
    help(watcher.clash)
    watcher.clash.by_summoner(my_region, summoner['id'])
    watcher.clash.by_team(my_region, team_id)
    watcher.clash.by_tournament(my_region, tournament_id)
    watcher.clash.tournament_by_team(my_region, team_id)
    watcher.clash.tournaments(my_region)
    help(watcher.data_dragon)
    versions = watcher.data_dragon.versions_for_region(my_region)
    watcher.data_dragon.champions(versions['n']['champion'], False, 'en_US')['data']
    watcher.data_dragon.items(versions['n']['item'], 'en_US')['data']
    watcher.data_dragon.languages(versions['n']['language'], 'en_US')
    watcher.data_dragon.maps(versions['n']['map'], 'en_US')
    watcher.data_dragon.masteries(versions['n']['mastery'], 'en_US')
    watcher.data_dragon.profile_icons(versions['n']['profileicon'], 'en_US')
    watcher.data_dragon.runes(versions['n']['rune'], 'en_US')['data']
    watcher.data_dragon.runes_reforged(versions['n']['rune'], 'en_US')
    help(watcher.league)
    solo_league = watcher.league.by_summoner(my_region, summoner['id'])[0]
    flex_league = watcher.league.by_summoner(my_region, summoner['id'])[1]
    watcher.league.by_id(my_region, solo_league['leagueId'])
    watcher.league.by_id(my_region, flex_league['leagueId'])
    watcher.league.by_summoner(my_region, summoner['id'])
    watcher.league.challenger_by_queue(my_region, solo_league['queueType'])
    watcher.league.challenger_by_queue(my_region, flex_league['queueType'])
    watcher.league.entries(my_region, solo_league['queueType'], solo_league['tier'], solo_league['rank'])
    watcher.league.entries(my_region, flex_league['queueType'], flex_league['tier'], flex_league['rank'])
    watcher.league.grandmaster_by_queue(my_region, solo_league['queueType'])
    watcher.league.grandmaster_by_queue(my_region, flex_league['queueType'])
    watcher.league.masters_by_queue(my_region, solo_league['queueType'])
    watcher.league.masters_by_queue(my_region, flex_league['queueType'])
    help(watcher.lol_status)
    watcher.lol_status.shard_data(my_region)
    help(watcher.match)
    watcher.match.matchlist_by_account(my_region, match['gameId'])
    watcher.match.by_id(my_region, match['gameId'])
    watcher.match.timeline_by_match(my_region, match['gameId'])
    help(watcher.spectator)
    watcher.spectator.by_summoner(my_region, summoner['id'])
    watcher.spectator.featured_games(my_region)
    help(watcher.third_party_code)
    watcher.third_party_code.by_summoner(my_region, summoner['id'])
    return Done

###############################################################################
def the_thing(username, api_key, how_many_matches):

    username = 'rickyyytan'
    api_key = 'RGAPI-00a3c89a-5bf5-4032-9711-ee04c92ab7aa'
    how_many_matches = 100

    # set start time and print statement
    start_time = dt.datetime.now()
    print(f'--- league_scraper_local.py started at: {start_time} ---')

    today = dt.datetime.now().strftime("%Y%m%d")
    today_id = dt.datetime.now().strftime("%Y-%m-%d")

    watcher = LolWatcher(api_key)
    my_region = 'na1'
    versions = watcher.data_dragon.versions_for_region(my_region)

    summoner_df = get_summoner_data(my_region, username, watcher)
    champions_complete_df = get_champions_data(versions, watcher)
    items_complete_df = get_items_data(versions, watcher)
    runes_complete_df = get_runes_data(versions, watcher)
    runes_reforged_complete_df = get_runes_reforged_data(versions, watcher)
    solo_league_df, flex_league_df = get_summoner_league_data(my_region, summoner_df, watcher)
    summoner_ranked_stats_df, summoner_matches_df, match_stats_complete_df = get_summoner_matches_data(my_region, summoner_df, how_many_matches, watcher)

    # rename the columns
    summoner_df.rename(
        columns={
            'id': 'summoner_id',
            'accountId': 'account_id',
            'puuid': 'puuid',
            'name': 'summoner_name',
            'profileIconId': 'profile_icon_id',
            'revisionDate': 'revision_timestamp',
            'summonerLevel': 'summoner_level'},
        inplace=True)
    champions_complete_df.rename(
        columns={
            'champion': 'champion_name',
            'version': 'version',
            'key': 'champion_id',
            'title': 'title',
            'blurb': 'blurb',
            'tags': 'tags',
            'partype': 'partype',
            'hp': 'hp',
            'hpperlevel': 'hp_per_level',
            'mp': 'mp',
            'mpperlevel': 'mp_per_level',
            'movespeed': 'movement_speed',
            'armor': 'armor',
            'armorperlevel': 'armor_per_level',
            'spellblock': 'spellblock',
            'spellblockperlevel': 'spellblock_per_level',
            'attackrange': 'attack_range',
            'hpregen': 'hp_regen',
            'hpregenperlevel': 'hp_regen_per_level',
            'mpregen': 'mp_regen',
            'mpregenperlevel': 'mp_regen_per_level',
            'crit': 'crit',
            'critperlevel': 'crit_per_level',
            'attackdamage': 'attack_damage',
            'attackdamageperlevel': 'attack_damager_per_level',
            'attackspeedperlevel': 'attack_speed_per_level',
            'attackspeed': 'attack_speed',
            'attack': 'attack',
            'defense': 'defense',
            'magic': 'magic',
            'difficulty': 'difficulty'},
        inplace=True)
    items_complete_df.rename(
        columns={
            'item_id': 'item_id',
            'name': 'item_name',
            'description': 'description',
            'plaintext': 'plaintext',
            'into': 'into',
            'tags': 'tags',
            'stats': 'stats',
            'from': 'from',
            'depth': 'depth',
            'effect': 'effect',
            'stacks': 'stacks',
            'consumed': 'consumed',
            'inStore': 'in_store',
            'hideFromAll': 'hide_from_all',
            'consumeOnFull': 'consume_on_full',
            'specialRecipe': 'special_recipe',
            'requiredChampion': 'required_champion',
            'base': 'base_price',
            'purchasable': 'is_purchasable',
            'total': 'total_price',
            'sell': 'sell_price'},
        inplace=True)
    runes_complete_df.rename(
        columns={
            'rune_id': 'rune_id',
            'name': 'rune_name',
            'description': 'description',
            'stats': 'stats',
            'tags': 'tags',
            'colloq': 'colloq',
            'plaintext': 'plaintext',
            'isrune': 'is_rune',
            'tier': 'tier',
            'type': 'type'},
        inplace=True)
    runes_reforged_complete_df.rename(
        columns={
            'rune_id': 'rune_id',
            'rune_key': 'rune_key',
            'rune_name': 'rune_name',
            'shortDesc': 'short_desc',
            'longDesc': 'long_desc',
            'runes_reforged_id': 'rune_reforged_id',
            'runes_reforged_key': 'rune_reforged_key',
            'runes_reforged_name': 'rune_reforged_name'},
        inplace=True)
    summoner_ranked_stats_df.rename(
        columns={
            'leagueId': 'league_id',
            'queueType': 'queue_type',
            'tier': 'tier',
            'rank': 'rank',
            'summonerId': 'summoner_id',
            'summonerName': 'summoner_name',
            'leaguePoints': 'league_points',
            'wins': 'wins',
            'losses': 'losses',
            'veteran': 'is_veteran',
            'inactive': 'is_inactive',
            'freshBlood': 'is_freshblood',
            'hotStreak': 'is_hotstreak'},
        inplace=True)
    summoner_matches_df.rename(
        columns={
        'platformId': 'platform_id',
        'gameId': 'game_id',
        'champion': 'champion_id',
        'queue': 'queue',
        'season': 'season',
        'timestamp': 'summoner_timestamp',
        'role': 'role',
        'lane': 'lane',
        'summoner_id': 'summoner_id'},
        inplace=True)
    match_stats_complete_df.rename(
        columns={
            'participantId': 'participant_id',
            'teamId': 'team_id',
            'champion': 'champion_id',
            'spell1': 'spell1',
            'spell2': 'spell2',
            'role': 'role',
            'lane': 'lane',
            'win': 'is_win',
            'item0': 'item0',
            'item1': 'item1',
            'item2': 'item2',
            'item3': 'item3',
            'item4': 'item4',
            'item5': 'item5',
            'item6': 'item6',
            'kills': 'kills',
            'deaths': 'deaths',
            'assists': 'assists',
            'largestKillingSpree': 'largest_killing_pree',
            'largestMultiKill': 'largest_multikill',
            'killingSprees': 'killing_sprees',
            'longestTimeSpentLiving': 'longest_time_spent_living',
            'doubleKills': 'doublekills',
            'tripleKills': 'triplekills',
            'quadraKills': 'quadrakills',
            'pentaKills': 'pentakills',
            'unrealKills': 'unrealkills',
            'totalDamageDealt': 'total_damage_dealt',
            'magicDamageDealt': 'magic_damage_dealt',
            'physicalDamageDealt': 'physical_damage_dealt',
            'trueDamageDealt': 'true_damage_dealt',
            'largestCriticalStrike': 'largest_critical_strike',
            'totalDamageDealtToChampions': 'total_damage_dealt_to_champions',
            'magicDamageDealtToChampions': 'magic_damage_dealt_to_champions',
            'physicalDamageDealtToChampions': 'physical_damage_dealt_to_champions',
            'trueDamageDealtToChampions': 'true_damage_dealt_to_champions',
            'totalHeal': 'total_heal',
            'totalUnitsHealed': 'total_units_healed',
            'damageSelfMitigated': 'damage_self_mitigated',
            'damageDealtToObjectives': 'damage_dealt_to_objectives',
            'damageDealtToTurrets': 'damage_dealt_to_turrets',
            'visionScore': 'vision_score',
            'timeCCingOthers': 'time_ccing_others',
            'totalDamageTaken': 'total_damage_taken',
            'magicalDamageTaken': 'magical_damage_taken',
            'physicalDamageTaken': 'physical_damage_taken',
            'trueDamageTaken': 'true_damage_taken',
            'goldEarned': 'gold_earned',
            'goldSpent': 'gold_spent',
            'turretKills': 'turret_kills',
            'inhibitorKills': 'inhibitor_kills',
            'totalMinionsKilled': 'total_minions_killed',
            'neutralMinionsKilled': 'neutral_minions_killed',
            'neutralMinionsKilledTeamJungle': 'neutral_minions_killed_team_jungle',
            'neutralMinionsKilledEnemyJungle': 'neutral_minions_killed_enemy_jungle',
            'totalTimeCrowdControlDealt': 'total_time_cc_dealt',
            'champLevel': 'champ_level',
            'visionWardsBoughtInGame': 'vision_wards_brought_in_game',
            'sightWardsBoughtInGame': 'sight_wards_brought_in_game',
            'wardsPlaced': 'wards_placed',
            'wardsKilled': 'wards_killed',
            'firstBloodKill': 'is_first_blood_kill',
            'firstBloodAssist': 'is_first_blood_assist',
            # 'firstTowerKill': 'is_first_tower_kill',
            # 'firstTowerAssist': 'is_first_tower_assist',
            'firstInhibitorKill': 'is_first_inhibitor_kill',
            'firstInhibitorAssist': 'is_first_inhibitor_assist',
            'combatPlayerScore': 'combat_player_score',
            'objectivePlayerScore': 'objective_player_score',
            'totalPlayerScore': 'total_player_score',
            'totalScoreRank': 'total_score_rank',
            'playerScore0': 'player_score0',
            'playerScore1': 'player_score1',
            'playerScore2': 'player_score2',
            'playerScore3': 'player_score3',
            'playerScore4': 'player_score4',
            'playerScore5': 'player_score5',
            'playerScore6': 'player_score6',
            'playerScore7': 'player_score7',
            'playerScore8': 'player_score8',
            'playerScore9': 'player_score9',
            'perk0': 'perk0',
            'perk0Var1': 'perk0_var1',
            'perk0Var2': 'perk0_var2',
            'perk0Var3': 'perk0_var3',
            'perk1': 'perk1',
            'perk1Var1': 'perk1_var1',
            'perk1Var2': 'perk1_var2',
            'perk1Var3': 'perk1_var3',
            'perk2': 'perk2',
            'perk2Var1': 'perk2_var1',
            'perk2Var2': 'perk2_var2',
            'perk2Var3': 'perk2_var3',
            'perk3': 'perk3',
            'perk3Var1': 'perk3_var1',
            'perk3Var2': 'perk3_var2',
            'perk3Var3': 'perk3_var3',
            'perk4': 'perk4',
            'perk4Var1': 'perk4_var1',
            'perk4Var2': 'perk4_var2',
            'perk4Var3': 'perk4_var3',
            'perk5': 'perk5',
            'perk5Var1': 'perk5_var1',
            'perk5Var2': 'perk5_var2',
            'perk5Var3': 'perk5_var3',
            'perkPrimaryStyle': 'perk_primary_style',
            'perkSubStyle': 'perl_sub_style',
            'statPerk0': 'stat_perk0',
            'statPerk1': 'stat_perk1',
            'statPerk2': 'stat_perk2',
            'gameId': 'game_id',
            'platformId': 'platform_id',
            'accountId': 'account_id',
            'summonerName': 'summoner_name',
            'summonerId': 'summoner_id',
            'currentPlatformId': 'current_platform_id',
            'currentAccountId': 'current_account_id',
            'matchHistoryUri': 'match_history_url',
            'profileIcon': 'profile_icon'},
        inplace=True)

    # define datatypes for each df with a list of tuples mapping datatypes for each column in each table
    summoner_df_datatypes = [
        ('summoner_id', 'object'),
        ('account_id', 'object'),
        ('puuid', 'object'),
        ('summoner_name', 'object'),
        ('profile_icon_id', 'int'),
        ('revision_timestamp', 'int'),
        ('summoner_level', 'int')]
    champions_complete_df_datatypes = [
        ('champion_name', 'object'),
        ('version', 'object'),
        ('champion_id', 'int'),
        ('title', 'object'),
        ('blurb', 'object'),
        ('tags', 'object'),
        ('partype', 'object'),
        ('hp', 'float'),
        ('hp_per_level', 'float'),
        ('mp', 'float'),
        ('mp_per_level', 'float'),
        ('movement_speed', 'float'),
        ('armor', 'float'),
        ('armor_per_level', 'float'),
        ('spellblock', 'float'),
        ('spellblock_per_level', 'float'),
        ('attack_range', 'float'),
        ('hp_regen', 'float'),
        ('hp_regen_per_level', 'float'),
        ('mp_regen', 'float'),
        ('mp_regen_per_level', 'float'),
        ('crit', 'float'),
        ('crit_per_level', 'float'),
        ('attack_damage', 'float'),
        ('attack_damager_per_level', 'float'),
        ('attack_speed_per_level', 'float'),
        ('attack_speed', 'float'),
        ('attack', 'float'),
        ('defense', 'float'),
        ('magic', 'float'),
        ('difficulty', 'float')]
    items_complete_df_datatypes = [
        ('item_id',	'int'),
        ('item_name', 'object'),
        ('description',	'object'),
        ('plaintext', 'object'),
        ('into', 'object'),
        ('tags', 'object'),
        ('stats', 'object'),
        ('from', 'object'),
        ('depth', 'object'),
        ('effect', 'object'),
        ('stacks', 'object'),
        ('consumed', 'object'),
        ('in_store', 'object'),
        ('hide_from_all', 'object'),
        ('consume_on_full', 'object'),
        ('special_recipe', 'object'),
        ('required_champion', 'object'),
        ('base_price', 'float'),
        ('is_purchasable',	'boolean'),
        ('total_price',	'float'),
        ('sell_price', 'float')]
    runes_complete_df_datatypes = [
        ('rune_id', 'int'),
        ('rune_name', 'object'),
        ('description', 'object'),
        ('stats', 'object'),
        ('tags', 'object'),
        ('colloq', 'object'),
        ('plaintext', 'object'),
        ('is_rune', 'boolean'),
        ('tier', 'int'),
        ('type', 'object')]
    runes_reforged_complete_df_datatypes = [
        ('rune_id', 'int'),
        ('rune_key', 'object'),
        ('rune_name', 'object'),
        ('short_desc', 'object'),
        ('long_desc', 'object'),
        ('rune_reforged_id', 'int'),
        # ('rune_reforged_key', 'object'),
        ('rune_reforged_name', 'object')]
    summoner_ranked_stats_df_datatypes = [
        ('league_id', 'object'),
        ('queue_type', 'object'),
        ('tier', 'object'),
        ('rank', 'object'),
        ('summoner_id', 'object'),
        ('summoner_name', 'object'),
        ('league_points', 'float'),
        ('wins', 'float'),
        ('losses', 'float'),
        ('is_veteran', 'boolean'),
        ('is_inactive', 'boolean'),
        ('is_freshblood', 'boolean'),
        ('is_hotstreak', 'boolean')]
    summoner_matches_df_datatypes = [
        ('platform_id', 'object'),
        ('game_id', 'int'),
        ('champion_id', 'int'),
        ('queue', 'int'),
        ('season', 'int'),
        ('summoner_timestamp', 'int'),
        ('role', 'object'),
        ('lane', 'object'),
        ('summoner_id', 'object')]
    match_stats_complete_df_datatypes = [
        ('participant_id', 'int'),
        ('team_id', 'int'),
        ('champion_id', 'int'),
        ('spell1', 'int'),
        ('spell2', 'int'),
        ('role', 'object'),
        ('lane', 'object'),
        ('is_win', 'boolean'),
        ('item0', 'int'),
        ('item1', 'int'),
        ('item2', 'int'),
        ('item3', 'int'),
        ('item4', 'int'),
        ('item5', 'int'),
        ('item6', 'int'),
        ('kills', 'float'),
        ('deaths', 'float'),
        ('assists', 'float'),
        ('largest_killing_pree', 'float'),
        ('largest_multikill', 'float'),
        ('killing_sprees', 'float'),
        ('longest_time_spent_living', 'float'),
        ('doublekills', 'float'),
        ('triplekills', 'float'),
        ('quadrakills', 'float'),
        ('pentakills', 'float'),
        ('unrealkills', 'float'),
        ('total_damage_dealt', 'float'),
        ('magic_damage_dealt', 'float'),
        ('physical_damage_dealt', 'float'),
        ('true_damage_dealt', 'float'),
        ('largest_critical_strike', 'float'),
        ('total_damage_dealt_to_champions', 'float'),
        ('magic_damage_dealt_to_champions', 'float'),
        ('physical_damage_dealt_to_champions', 'float'),
        ('true_damage_dealt_to_champions', 'float'),
        ('total_heal', 'float'),
        ('total_units_healed', 'float'),
        ('damage_self_mitigated', 'float'),
        ('damage_dealt_to_objectives', 'float'),
        ('damage_dealt_to_turrets', 'float'),
        ('vision_score', 'float'),
        ('time_ccing_others', 'float'),
        ('total_damage_taken', 'float'),
        ('magical_damage_taken', 'float'),
        ('physical_damage_taken', 'float'),
        ('true_damage_taken', 'float'),
        ('gold_earned', 'float'),
        ('gold_spent', 'float'),
        ('turret_kills', 'float'),
        ('inhibitor_kills', 'float'),
        ('total_minions_killed', 'float'),
        ('neutral_minions_killed', 'float'),
        ('neutral_minions_killed_team_jungle', 'float'),
        ('neutral_minions_killed_enemy_jungle', 'float'),
        ('total_time_cc_dealt', 'float'),
        ('champ_level', 'float'),
        ('vision_wards_brought_in_game', 'float'),
        ('sight_wards_brought_in_game', 'float'),
        ('wards_placed', 'float'),
        ('wards_killed', 'float'),
        ('is_first_blood_kill', 'boolean'),
        ('is_first_blood_assist', 'boolean'),
        # ('is_first_tower_kill', 'boolean'),
        # ('is_first_tower_assist', 'boolean'),
        ('is_first_inhibitor_kill', 'boolean'),
        ('is_first_inhibitor_assist', 'boolean'),
        ('combat_player_score', 'float'),
        ('objective_player_score', 'float'),
        ('total_player_score', 'float'),
        ('total_score_rank', 'float'),
        ('player_score0', 'float'),
        ('player_score1', 'float'),
        ('player_score2', 'float'),
        ('player_score3', 'float'),
        ('player_score4', 'float'),
        ('player_score5', 'float'),
        ('player_score6', 'float'),
        ('player_score7', 'float'),
        ('player_score8', 'float'),
        ('player_score9', 'float'),
        ('perk0', 'float'),
        ('perk0_var1', 'float'),
        ('perk0_var2', 'float'),
        ('perk0_var3', 'float'),
        ('perk1', 'float'),
        ('perk1_var1', 'float'),
        ('perk1_var2', 'float'),
        ('perk1_var3', 'float'),
        ('perk2', 'float'),
        ('perk2_var1', 'float'),
        ('perk2_var2', 'float'),
        ('perk2_var3', 'float'),
        ('perk3', 'float'),
        ('perk3_var1', 'float'),
        ('perk3_var2', 'float'),
        ('perk3_var3', 'float'),
        ('perk4', 'float'),
        ('perk4_var1', 'float'),
        ('perk4_var2', 'float'),
        ('perk4_var3', 'float'),
        ('perk5', 'float'),
        ('perk5_var1', 'float'),
        ('perk5_var2', 'float'),
        ('perk5_var3', 'float'),
        ('perk_primary_style', 'float'),
        ('perl_sub_style', 'float'),
        ('stat_perk0', 'float'),
        ('stat_perk1', 'float'),
        ('stat_perk2', 'float'),
        ('game_id', 'float'),
        ('platform_id', 'object'),
        ('account_id', 'object'),
        ('summoner_name', 'object'),
        ('summoner_id', 'object'),
        ('current_platform_id', 'object'),
        ('current_account_id', 'object'),
        ('match_history_url', 'object'),
        ('profile_icon', 'int')]

    # define the datatypes
    summoner_df = convert_column_datatypes(summoner_df, summoner_df_datatypes)
    champions_complete_df = convert_column_datatypes(champions_complete_df, champions_complete_df_datatypes)
    items_complete_df = convert_column_datatypes(items_complete_df, items_complete_df_datatypes)
    runes_complete_df = convert_column_datatypes(runes_complete_df, runes_complete_df_datatypes)
    runes_reforged_complete_df = convert_column_datatypes(runes_reforged_complete_df, runes_reforged_complete_df_datatypes)
    summoner_ranked_stats_df = convert_column_datatypes(summoner_ranked_stats_df, summoner_ranked_stats_df_datatypes)
    summoner_matches_df = convert_column_datatypes(summoner_matches_df, summoner_matches_df_datatypes)
    match_stats_complete_df = convert_column_datatypes(match_stats_complete_df, match_stats_complete_df_datatypes)

    # last things to change
    # drop duplicate columns
    champions_complete_df.drop(columns=['id', 'name'], inplace=True)
    runes_reforged_complete_df.drop(columns=['rune_reforged_key'], inplace=True)
    # convert unix ms timestamp to datetime
    summoner_df['revision_timestamp'] = pd.to_datetime(
        summoner_df['revision_timestamp'], unit='ms')
    summoner_matches_df['summoner_timestamp'] = pd.to_datetime(
        summoner_matches_df['summoner_timestamp'], unit='ms')

    # list of tuples to iterate through for writing out the csv's
    final_dfs = [('summoner', summoner_df),
                 ('champions', champions_complete_df),
                 ('items', items_complete_df),
                 ('runes', runes_complete_df),
                 ('runes_reforged', runes_reforged_complete_df),
                 ('summoner_ranked_stats', summoner_ranked_stats_df),
                 ('summoner_matches', summoner_matches_df),
                 ('match_stats', match_stats_complete_df)]

    # s3_bucket = 'league'

    # write out each dataframe to both current and historic files in output folder
    for final_df in final_dfs:
        # for local
        file = final_df[0]
        file_df = final_df[1]
        filename_historic = f'/Users/brianshin/brian/tinker/brian_dwh/league/output/{file}/{file}_'+today_id.replace('-','')+'.csv'
        filename = f'/Users/brianshin/brian/tinker/brian_dwh/league/output/current/{file}.csv'
        output_filename = f'{file}/{file}_{today}.csv'
        file_df.to_csv(filename_historic, index=False, encoding='utf-8')
        file_df.to_csv(filename, index=False, encoding='utf-8')
        # for s3
        # load_s3(s3_bucket=s3_bucket, input_filename=filename, output_filename=output_filename)

    time = (dt.datetime.now() - start_time)
    print(f"--- league_scraper_local.py done in {dt.timedelta(seconds=time.seconds)} at {dt.datetime.now()} ---")

###############################################################################
# u know what it do
if __name__ == '__main__':
    # parse username, api key, and number of matches to pull from args
    username, api_key, how_many_matches = get_username_and_api_key()
    the_thing(username, api_key, how_many_matches)
