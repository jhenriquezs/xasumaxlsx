import sqlite3
import glob
from sqlite3 import Error
from enum import Enum

class Modo(Enum):
    osu = 0
    taiko = 1
    ctb = 2
    osumania = 3

class Genre(Enum):
    any = 0
    unspecified = 1
    videogame = 2
    anime = 3
    rock = 4
    pop = 5
    other = 6
    novelty = 7
    hiphop = 9
    electronic = 10
    folk = 13

class Language(Enum):
    any = 0
    other = 1
    english = 2
    japanese = 3
    chinese = 4
    instrumental = 5
    korean = 6
    french = 7
    german = 8
    swedish = 9
    spanish = 10
    italian = 11


# create connection with the database or create another one
def create_connection(db_name):
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        col_table_statement = """CREATE TABLE IF NOT EXISTS beatmaps (beatmap_id int PRIMARY KEY,hyperlink text, approved text, 
                              mode int, total_length int,hit_length int,version text,diff_size float, video int,
                              diff_overall float ,diff_approach float ,diff_drain float ,count_normal int ,
                              count_slider int,count_spinner int, bpm int,favourite_count int, storyboard int,
                              rating float,playcount int,passcount int, max_combo int,diff_aim float ,diff_speed float ,
                              difficultyrating float, artist text, artist_unicode text, title text,title_unicode text, 
                              creator text,creator_id int,top_score int, top_username text, top_userid int, top_maxcombo int, 
                              top_date date,top_rank int, top_pp float, top_FC text, top_8_score int, top_8_username text, 
                              top_8_userid int, top_8_maxcombo int, top_8_date date, top_8_rank int, top_8_pp float, 
                              top_8_FC text, top_25_score int, top_25_username text, top_25_userid int, top_25_maxcombo int, 
                              top_25_date date, top_25_rank int, top_25_pp float, top_25_FC text, top_50_score int, 
                              top_50_username text, top_50_userid int, top_50_maxcombo int, top_50_date date, 
                              top_50_rank int, top_50_pp float, top_50_FC text, top_100_score int, top_100_username text, 
                              top_100_userid int, top_100_maxcombo int, top_100_date date, top_100_rank int, 
                              top_100_pp float, top_100_FC text, no_mod_score int, no_mod_username text, no_mod_userid int, 
                              no_mod_maxcombo int, no_mod_date date, no_mod_rank int, no_mod_pp float, no_mod_FC text, 
                              no_mod_8_score int, no_mod_8_username text, no_mod_8_userid int, no_mod_8_maxcombo int, 
                              no_mod_8_date date, no_mod_8_rank int, no_mod_8_pp float, no_mod_8_FC text, no_mod_25_score int, 
                              no_mod_25_username text, no_mod_25_userid int, no_mod_25_maxcombo int, no_mod_25_date date, 
                              no_mod_25_rank int, no_mod_25_pp float, no_mod_25_FC text, no_mod_50_score int,
                              no_mod_50_username text, no_mod_50_userid int, no_mod_50_maxcombo int, no_mod_50_date date, 
                              no_mod_50_rank int, no_mod_50_pp float, no_mod_50_FC text, no_mod_100_score int, 
                              no_mod_100_username text, no_mod_100_userid int, no_mod_100_maxcombo int, no_mod_100_date date, 
                              no_mod_100_rank int, no_mod_100_pp float, no_mod_100_FC text,beatmapset_id int,submit_date date ,
                              approved_date date,last_update date, source text,tags text,genre_id int,language_id int, 
                              download_unavailable int,audio_unavailable int, packs text, file_md5 text)"""
        cur.execute(col_table_statement)
        conn.commit()
        conn.row_factory = dict_factory
    except Error as e:
        print(e)
    return conn

# insert only 1 element of the beatmaps array
def insert(conn, cursor, dict_element):
    items = list(dict_element.items())
    insert_statement = """INSERT OR IGNORE INTO beatmaps (\
{0[0]}, {1[0]}, {2[0]}, {3[0]}, {4[0]}, {5[0]}, {6[0]}, {7[0]}, {8[0]}, {9[0]}, {10[0]}, {11[0]}, {12[0]}, {13[0]}, \
{14[0]}, {15[0]}, {16[0]}, {17[0]}, {18[0]}, {19[0]}, {20[0]}, {21[0]}, {22[0]}, {23[0]}, {24[0]}, {25[0]}, {26[0]}, \
{27[0]}, {28[0]}, {29[0]}, {30[0]}, {31[0]}, {32[0]}, {33[0]}, {34[0]}, {35[0]}, {36[0]}, {37[0]}, {38[0]}, {39[0]}, {40[0]}, {41[0]} \
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".format(*items)
    cursor.execute(insert_statement, list(dict_element.values()))
    conn.commit()

# Update table values
def update(conn, cursor, dict, beatmap_id):
    query = "UPDATE beatmaps SET"
    for k,v in dict.items():
        if (type(v) == str):
            v = v.replace('"','""')
        changes = """ `{}`="{}",""".format(k,v)
        query += changes
    query = query[:-1] + " WHERE beatmap_id = "+ str(beatmap_id) + ";"
    cursor.execute(query)
    conn.commit()

# Obtain the databases names in the current folder
def get_db_names():
    return glob.glob("*.db")

# insert in one statement all the values in the beatmaps dictionary
def insert_chunk(conn, cursor, array_dicts, loved):
    llaves = list(array_dicts[0].items())
    insert_statement = """INSERT OR IGNORE INTO beatmaps (\
    {0[0]}, {1[0]}, {2[0]}, {3[0]}, {4[0]}, {5[0]}, {6[0]}, {7[0]}, {8[0]}, {9[0]}, {10[0]}, {11[0]}, {12[0]}, {13[0]}, \
    {14[0]}, {15[0]}, {16[0]}, {17[0]}, {18[0]}, {19[0]}, {20[0]}, {21[0]}, {22[0]}, {23[0]}, {24[0]}, {25[0]}, {26[0]}, \
    {27[0]}, {28[0]}, {29[0]}, {30[0]}, {31[0]}, {32[0]}, {33[0]}, {34[0]}, {35[0]}, {36[0]}, {37[0]}, {38[0]}, {39[0]}, 
    {40[0]}, {41[0]}, hyperlink) values """.format(*llaves)
    index_dict = getIndexOfTuple(llaves, 0, ["mode", "beatmap_id", "beatmapset_id"])
    for beatmap in array_dicts:
        # filter double quotes
        flag = False
        if (int(beatmap["approved"]) == 1):
            flag = True
            beatmap["approved"] = "ranked"
        elif (int(beatmap["approved"]) == 2):
            flag = True
            beatmap["approved"] = "approved"
        elif (int(beatmap["approved"]) == 4 and loved):
            flag = True
            beatmap["approved"] = "loved"
        try:
            beatmap["mode"] = Modo(int(beatmap["mode"])).name
            beatmap["language_id"] = Language(int(beatmap["language_id"])).name
            beatmap["genre_id"] = Genre(int(beatmap["genre_id"])).name
        except:
            pass
        if (flag):
            valores = []
            for element in beatmap.values():
                if (element):
                    valores.append(element.replace('"','""'))
                else:
                    valores.append(element)
            link = "https://osu.ppy.sh/beatmapsets/" + valores[index_dict["beatmapset_id"]] + "#"+valores[index_dict["mode"]]+"/" + valores[index_dict["beatmap_id"]]
            temp_statement = """("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", \
            "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", \
            "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", " """.format(*valores) + link + """ " ),"""
            insert_statement += temp_statement
    query = insert_statement[:-1] + ";"
    cursor.execute(query)
    conn.commit()

# function for parsing SELECT queries
def dict_factory(cursor, row):
    d={}
    for index, col in enumerate(cursor.description):
        d[col[0]] = row[index]
    return d

# Stops iterating through the list as soon as it finds the value
def getIndexOfTuple(l, index, value):
    dictionary = {}
    for pos,t in enumerate(l):
        if t[index] in value:
            dictionary[t[index]] = pos
        if len(dictionary) == len(value):
            return dictionary

    # Matches behavior of list.index
    raise ValueError("faltan argumentos, se tiene: \ndictionary: "+dictionary+"\nllaves: "+ str(value))

# Add Columns with user info
def create_column(conn, user, column_names, lista_usuarios):
    cur = conn.cursor()
    # query = "PRAGMA table_info(beatmaps)"
    # cur.execute(query)
    # column_names = cur.fetchall()
    id = str(user['id'])
    username = str(user['username'])
    if (id in lista_usuarios.keys()):
        for names in lista_usuarios[id]:
            if ("enabled_mods_"+names in column_names):
                return True
        if (username in lista_usuarios[id]):
            lista_usuarios[id] = lista_usuarios[id].append(username)
    else:
        lista_usuarios[id] = [username]
        if ("enabled_mods_" + username in column_names):
            return True
    # for cols in reversed(column_names):
    #     if (string == cols['name']):
    #         return True
    #     if ("file_md5" == cols['name']):
    #         break
    columns = [("score_", "int"),("accuracy_", "float"),("combo_", "int"),("count300_", "int"),("count100_", "int"),
               ("count50_", "int"),("countmiss_", "int"),("date_", "date"),("rank_", "int"),("pp_", "float"),
               ("FC_", "text"),("enabled_mods_", "text")]
    for names in columns:
        query = "ALTER TABLE beatmaps ADD COLUMN `" + names[0] + username + "` " + names[1] + ";"
        cur.execute(query)
        conn.commit()

def get_columns(database):
    conn = create_connection(database)
    cur = conn.cursor()
    cur.execute("""PRAGMA table_info(beatmaps)""")
    column_names = cur.fetchall()
    conn.close()
    return list(data['name'] for data in column_names)




