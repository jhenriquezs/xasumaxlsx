import json, requests, time, sys
import db_util as dbu

# get all "ranked" beatmaps from the base date, every request have a limit of 500 beatmaps/s
def get_beatmaps(mode, key, conv, conn):
    base_date = '2020-02-18 17:46:31'
    converted = ""
    if (conv):
        converted = '&a=1'
    try:
        data = requests.get("https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ base_date +"&m="+str(mode)+"&limit=500"+converted).json()
    except:
        sys.exit("Your api key is wrong!!!!!")
    # should delete this... just not right now
    beatmaps_total = data
    dbu.insert_chunk(conn, conn.cursor(), data)

    while (len(data)!= 0):
        if (len(data) < 500):
            break
        print("beatmaps hasta el momento: " + str(len(beatmaps_total)))
        last_map = data[-1]
        time.sleep(1)
        data = requests.get("https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ last_map['approved_date'] +"&m="+str(mode)+"&limit=500"+converted).json()
        # check if the last beatmap of the previous request is inside in the new request(this helps to calculate the offset)
        if (data[0]['approved_date'] == last_map['approved_date']):
            fecha = last_map['approved_date']
            cont = 0
            while (fecha == last_map['approved_date']):
                if (data[cont] == last_map):
                    if (data[cont+1:] == []):
                        print("Aparentemente se subieron tantos mapas al mismo tiempo como la cantidad de queries")
                        break
                    else:
                        beatmaps_total = beatmaps_total + data[cont+1:]
                        dbu.insert_chunk(conn, conn.cursor(), data[cont+1:])
                cont+=1
                fecha = data[cont]['approved_date']
        else:
            beatmaps_total= beatmaps_total + data
            dbu.insert_chunk(conn, conn.cursor(), data)
    print("beatmaps en total: "+ str(len(beatmaps_total)))

# get the scores from all the beatmaps, every request is 100 scores/s and 1s per beatmap
def get_scores(key, mode, no_mod, conn):
    tipo = "top_"
    mods = ""
    # Obtaining all the beatmap_id and max_combo from the database
    cur = conn.cursor()
    cur.execute("SELECT beatmap_id, max_combo from beatmaps")
    beatmaps_dict = cur.fetchall()
    if (no_mod):
        mods = "&mods=(0|16384)"
        tipo = "no_mod_"
    contador = 0
    for beatmap in beatmaps_dict:
        time.sleep(1)
        id = beatmap['beatmap_id']
        query = "https://osu.ppy.sh/api/get_scores?k="+ key +"&b="+ str(id) +"&limit=100&m=" + str(mode) + mods
        score = requests.get(query).json()
        if (score == []):
            print('no se encontro score en: ' + str(beatmap['beatmap_id']))
            pass
        else:
            if (len(score) < 8):
                refresh(beatmap, tipo, [score[0]], conn, id)
            elif (8 <= len(score) < 25):
                refresh(beatmap, tipo ,[score[0], score[7]], conn, id)
            elif (25 <= len(score) < 50):
                refresh(beatmap, tipo ,[score[0], score[7], score[24]], conn, id)
            elif (50 <= len(score) < 100):
                refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49]], conn, id)
            else:
                refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49], score[99]], conn, id)
        contador += 1
        if (contador%500 == 0):
            print( str(contador) + " beatmaps actualizados de " + str(len(beatmaps_dict)))

# Aux function for get_scores. Updates database scores
def refresh(beatmap, tipo ,scores, conn, id):
    rank = ["", "8_", "25_", "50_", "100_"]
    contador = 0
    send = {}
    for puntuacion in scores:
        combo = "NO"
        if (beatmap["max_combo"] == puntuacion["maxcombo"]):
            combo = "YES"
        send.update({tipo + rank[contador] + 'score': puntuacion["score"],
                        tipo + rank[contador] + 'username': puntuacion["username"],
                        tipo + rank[contador] + 'userid': puntuacion["user_id"],
                        tipo + rank[contador] + 'maxcombo': puntuacion["maxcombo"],
                        tipo + rank[contador] + 'date': puntuacion["date"],
                        tipo + rank[contador] + 'rank': puntuacion["rank"],
                        tipo + rank[contador] + 'pp': puntuacion["pp"],
                        tipo + rank[contador] + 'FC': combo})

        # beatmap.update({tipo + rank[contador] + '_score': puntuacion["score"],
        #                 tipo + rank[contador] + '_username': puntuacion["username"],
        #                 tipo + rank[contador] + '_user_id': puntuacion["user_id"],
        #                 tipo + rank[contador] + '_maxcombo': puntuacion["maxcombo"],
        #                 tipo + rank[contador] + '_date': puntuacion["date"],
        #                 tipo + rank[contador] + '_rank': puntuacion["rank"],
        #                 tipo + rank[contador] + '_pp': puntuacion["pp"],
        #                 tipo + rank[contador] + '_FC?': combo})
        contador += 1
    dbu.update(conn, conn.cursor(), send, id)

# Call all beatmaps from the database
def call_beatmaps(conn):
    cur = conn.cursor()
    cur.execute("SELECT * from beatmaps")
    return cur.fetchall()

def create_json(filename, data):
    print(filename)
    with open(filename, 'w') as json_file:
        json.dump(data, json_file)
    return None

def open_json(filename):
    with open(filename) as json_file:
        datos = json.load(json_file)
    return datos
