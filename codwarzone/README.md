# Call of Duty Warzone Stat Tracker #

### Intro ####
I wanted to make my own ETL with the endstate being a database that is consistently being updated. I needed a constantly growing data source, so I chose to track my Call of Duty Warzone stats, since I've been playing a lot during quarantine. Also, I wanted to blur the lines between playing and working (after all, I need to play for the sake of new data lol). For now, I've made my database on SQLite to mostly test concepts out. As for the ETL itself, I've broken it down into 4 stages: the Extractor stage (scraping the data off cod.tracker with BeautifulSoup), the RawData stage (inserting the raw data as text to the db), the Staging stage (the transform layer), and finally the Production stage (upserting to the final tables).


## Data Model ##
| will | fill  |  later |
|---|---|---|
|   |   |   |
|   |   |   |

## Stages ##
### 1. Extractor

- brian_dwh/codwarzone/extractor/warzone_scraper_local.py

- This python script uses urllib requests (for static pages) and selenium on a chrome driver (for dynamic content) to pull profile and game stats from cod.tracker (https://cod.tracker.gg/warzone/profile/battlenet/gs25%2311901/overview) using BeautifulSoup and produces 3 pandas dataframes.

  + profile
    - a snapshot of my profile stats (https://cod.tracker.gg/warzone/profile/battlenet/gs25%2311901/overview)
  + game_details
    - the previewed game details from my match history (https://cod.tracker.gg/warzone/profile/battlenet/gs25%2311901/matches)
  + game_stats
    - the complete stats from a given game (https://cod.tracker.gg/warzone/match/14818358671278315261?handle=rickytan)


  
    <img src="output/images/codtracker.png" width="875"/>

    <img src="output/images/scraper.png" height="500"/> <img src="output/images/dataframes.png" height="500"/>
  
  
### 2. RawData

### 3. Staging
### 4. Prod




#### some misc notes and challenges ####

+ In creating the prod table, sqlite doesn't support having multiple keys in the ON CONFLICT clause. this makes things annoying because if I want to ingest the games of my friends, there can only be one row for a game. This means the game_id is no longer a unique primary key. I thought of making game_id as a hash of the game_id and the gamer_id, since that's the true unique constraint and grain of the game_details_dim and game_stats_dim table, but sqlite also doesn't have any hashing functions built in (was hoping for SHA1 or MD5). For now, I have a few possible solutions:
  - game_id = game_id || "_" || gamer_id and separate column for game_url_id
  - keep game_id and make new pk for game_details_id and game_stats_id of game_id = game_id || "_" || gamer_id 
