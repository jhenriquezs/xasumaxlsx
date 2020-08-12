import json, requests, time, sys
import db_util as dbu
import datetime, threading
from enum import Enum
from queue import Queue
import pathlib

user_score_beatmaps_api = Queue()
contador = Queue()

class Mod(Enum):
    NoMod = 0
    NoFail = 1
    Easy = 2
    TouchDevice = 4
    Hidden = 8
    HardRock = 16
    SuddenDeath = 32
    DoubleTime = 64
    Relax = 128
    HalfTime = 256
    Nightcore = 512
    Flashlight = 1024
    Autoplay = 2048
    SpunOut = 4096
    AutoPilot = 8192
    Perfect = 16384
    ScoreV2 = 536870912


def retry_query(query):
    while True:
        try:
            data = requests.get(query)
            data = data.json()
            if (data != None and ("error" not in data)):
                break
            else:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - No se ha podido recibir datos con el formato correcto: ")
                time.sleep(2)
        except requests.ConnectionError as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Connection Error: ")
            print(str(e))
            print("Ejecucion de la proxima query en 30s: ")
            time.sleep(30)
        except requests.Timeout as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Timeout Error: ")
            print(str(e))
            print("Ejecucion de la proxima query en 30s: ")
            time.sleep(30)
        except requests.RequestException as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - General error!!!!:")
            print(str(e))
            print("Ejecucion de la proxima query en 30s: ")
            time.sleep(30)
        except json.decoder.JSONDecodeError as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - json format error!!!!:")
            print(str(e))
            print("Ejecucion de la proxima query en 30s: ")
            time.sleep(30)
    return data

# get all "ranked" beatmaps from the base date, every request have a limit of 500 beatmaps/s

def get_beatmaps(mode, key, conv, conn, base_date, end_date, loved):
    converted = ""
    if (conv):
        converted = '&a=1'
    try:
        data = requests.get("https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ base_date +"&m="+str(mode)+"&limit=500"+converted).json()
    except:
        sys.exit("Your api key is wrong!!!!!")
    # should delete this... just not right now
    beatmaps_total = data
    dbu.insert_chunk(conn, conn.cursor(), data, loved)

    while (len(data)!= 0):
        if (len(data) < 500):
            break
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - beatmaps hasta el momento: " + str(len(beatmaps_total)))
        last_map = data[-1]
        time.sleep(0.66)
        before_last_day = last_map['approved_date'].split()[0]
        day_before_object = datetime.datetime.strptime(before_last_day, '%Y-%m-%d') - datetime.timedelta(days=1)
        query = "https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ str(day_before_object) +"&m="+str(mode)+"&limit=500"+converted
        data = retry_query(query)
        # check if the last beatmap of the previous request is inside in the new request(this helps to calculate the offset)
        if (data[0]['approved_date'] == last_map['approved_date']):
            fecha = last_map['approved_date']
            cont = 0
            while (fecha == last_map['approved_date']):
                if (data[cont] == last_map):
                    if (data[cont+1:] == []):
                        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                              +" - Aparentemente se subieron tantos mapas al mismo tiempo como la cantidad de queries")
                        break
                    else:
                        beatmaps_total = beatmaps_total + data[cont+1:]
                        dbu.insert_chunk(conn, conn.cursor(), data[cont+1:], loved)
                cont+=1
                fecha = data[cont]['approved_date']
        else:
            beatmaps_total= beatmaps_total + data
            dbu.insert_chunk(conn, conn.cursor(), data, loved)
        if (datetime.datetime.strptime(last_map['approved_date'],"%Y-%m-%d %H:%M:%S") > end_date):
            break
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - beatmaps en total: "+ str(len(beatmaps_total)))

# get the scores from all the beatmaps, every request is 100 scores/s and 1s per beatmap
def get_scores(key, mode, no_mod, fecha, fecha_end, setting, database):
    tipo = "top_"
    mods = ""
    # Obtaining all the beatmap_id and max_combo from the database
    conn = dbu.create_connection(database)
    cur = conn.cursor()
    if (setting):
        limits = format_settings(setting)
        db_query = "SELECT beatmap_id, max_combo from beatmaps WHERE approved_date BETWEEN '" + fecha + "' AND '"+ fecha_end +"'AND " + limits + " ORDER BY `approved_date` ASC;"
    else:
        db_query = "SELECT beatmap_id, max_combo from beatmaps WHERE approved_date BETWEEN '" + fecha + "' AND '"+ fecha_end +"' ORDER BY `approved_date` ASC;"
    cur.execute(db_query)
    beatmaps_dict = cur.fetchall()
    if (no_mod):
        mods = "&mods=(0|16384)"
        tipo = "no_mod_"
    contador.put(0)
    lenght = len(beatmaps_dict)
    conn.close()
    t1_api = threading.Thread(target=query_api, args=(beatmaps_dict, key, mode, mods))
    t1_api.start()
    while (t1_api.isAlive() or (user_score_beatmaps_api.qsize() > 0)):
        if (user_score_beatmaps_api.qsize() == 0):
            time.sleep(10)
        else:
            t2 = threading.Thread(target=query_api_to_bd, args=(tipo, lenght, database))
            t2.start()
            t2.join()
    t1_api.join()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ' - Se han actualizado todos los scores')
    # for beatmap in beatmaps_dict:
    #     id = beatmap['beatmap_id']
    #     query = "https://osu.ppy.sh/api/get_scores?k="+ key +"&b="+ str(id) +"&limit=100&m=" + str(mode) + mods
    #     score = retry_query(query)
    #     if (score == []):
    #         print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    #               +' - no se encontro score en: '+str(beatmap['beatmap_id']))
    #         time.sleep(0.33)
    #         pass
    #     else:
    #         try:
    #             if (len(score) < 8):
    #                 refresh(beatmap, tipo, [score[0]], conn, id)
    #             elif (8 <= len(score) < 25):
    #                 refresh(beatmap, tipo ,[score[0], score[7]], conn, id)
    #             elif (25 <= len(score) < 50):
    #                 refresh(beatmap, tipo ,[score[0], score[7], score[24]], conn, id)
    #             elif (50 <= len(score) < 100):
    #                 refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49]], conn, id)
    #             else:
    #                 refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49], score[99]], conn, id)
    #         except:
    #             print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - Error")
    #             print(sys.exc_info())
    #             print(sys.exc_info()[0])
    #             print(sys.exc_info()[1])
    #     contador += 1
    #     if (contador%500 == 0):
    #         print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - " + str(contador) + " beatmaps actualizados de "
    #               + str(len(beatmaps_dict)))
    #         if (contador%20000 == 0):
    #             conn.close()
    #             conn = dbu.create_connection(database)
    contador.get()


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

# Add User scores to the database
def user_scores(key, fecha, fecha_end, user, mode, setting, database):
    user = list({v['username']:v for v in user}.values())
    columnas = dbu.get_columns(database)
    directorio = pathlib.Path().absolute()
    pathlib.Path(str(directorio)+'\\database').mkdir(parents=True, exist_ok=True)
    try:
        lista_usuarios = open_json(str(directorio)+"\\database\\users_"+database+".json")
    except:
        lista_usuarios = {}
    conn = dbu.create_connection(database)
    for dict in user:
        try:
            print(dict['username'])
            dbu.create_column(conn, dict,columnas, lista_usuarios)
        except:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
            print(sys.exc_info())
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
    create_json(str(directorio)+"\\database\\users_"+database+".json",lista_usuarios)
    print(user)
    cur = conn.cursor()
    if (setting):
        limits = format_settings(setting)
        db_query = "SELECT beatmap_id, max_combo, hyperlink, no_mod_score from beatmaps WHERE approved_date BETWEEN '" + fecha + "' AND '"+ fecha_end +"' AND " + limits + " ORDER BY `approved_date` ASC;"
    else:
        db_query = "SELECT beatmap_id, max_combo, hyperlink, no_mod_score from beatmaps WHERE approved_date BETWEEN '" + fecha + "' AND '"+ fecha_end +"' ORDER BY `approved_date` ASC;"
    cur.execute(db_query)
    beatmaps_dict = cur.fetchall()
    # cont = 0
    # second_counter = 0

    if (contador.qsize() == 0):
        contador.put(0)

    conn.close()
    t1_api = threading.Thread(target=query_user_api, args=(beatmaps_dict, key, mode, user))
    t1_api.start()
    while (t1_api.isAlive() or (user_score_beatmaps_api.qsize() > 0)):
        if (user_score_beatmaps_api.qsize() == 0):
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ' - Esperando a que se encuentren mas entradas para ingresar. (Reintentando en 10 segundos mas)')
            time.sleep(10)
        else:
            t2 = threading.Thread(target=query_user_api_to_bd, args=[database])
            t2.start()
            print(datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M") + ' - Ingresando nuevas entradas para usuarios')
            t2.join()
    t1_api.join()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ' - Se han actualizado todos los scores')
    # for beatmap in beatmaps_dict:
    #     id = beatmap['beatmap_id']
    #     for player in user:
    #         second_counter += 1
    #         query = "https://osu.ppy.sh/api/get_scores?k=" + key + "&b=" + str(id) + "&m=" + str(mode)+ "&u=" + str(player['id'])
    #         score = retry_query(query)
    #         if (score == []):
    #             print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+' - no se encontro score en: ' + str(beatmap['hyperlink']) +
    #                   ' para el player: '+ player['username'])
    #             time.sleep(0.33)
    #             pass
    #         else:
    #             combo = "NO"
    #             try:
    #                 if (str(beatmap["max_combo"]) == str(score[0]["maxcombo"])):
    #                     combo = "YES"
    #             except:
    #                 print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
    #                 print(sys.exc_info())
    #                 print(sys.exc_info()[0])
    #                 print(sys.exc_info()[1])
    #                 print("beatmap max_combo: ")
    #                 print(beatmap["max_combo"])
    #                 print("contenido score: ")
    #                 print(score)
    #                 print("score maxcombo: ")
    #                 print(score[0]["maxcombo"])
    #             send_dict = {'top_nomod_'+player['username'] : beatmap["no_mod_score"],
    #                          'score_'+player['username'] : score[0]['score'],
    #                          'maxcombo_'+player['username'] : beatmap["max_combo"],
    #                          'combo_'+player['username'] : score[0]['maxcombo'],
    #                          'date_'+player['username'] : score[0]['date'],
    #                          'rank_'+player['username'] : score[0]['rank'],
    #                          'pp_'+player['username'] : score[0]['pp'],
    #                          'FC_'+player['username'] : combo,
    #                          'countmiss_'+player['username']: score[0]['countmiss'],
    #                          'enabled_mods_'+player['username']: translate_mods(score[0]['enabled_mods'])}
    #             try:
    #                 dbu.update(conn, conn.cursor(), send_dict, id)
    #             except:
    #                 print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
    #                 print(sys.exc_info())
    #                 print(sys.exc_info()[0])
    #                 print(sys.exc_info()[1])
    #     cont += 1
    #     if (cont % 500 == 0):
    #         print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - "+str(cont) + " beatmaps actualizados de " +
    #               str(len(beatmaps_dict)))
    #         if (second_counter%20000 == 0):
    #             conn.close()
    #             conn = dbu.create_connection(database)
    contador.get()

# Return True if user is invalid
def validating_user(user, key):
    player_list = user.split(",")
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - Validando usuarios")
    player_result = []
    for player in player_list:
        time.sleep(0.5)
        query = "https://osu.ppy.sh/api/get_user?k="+key+"&u="+str(player)
        result = retry_query(query)
        if (result == []):
            return {}
        else:
            player_result.append({'id':result[0]['user_id'], 'username':result[0]['username']})
    return player_result

# Call all beatmaps from the database
def call_beatmaps(conn, date, end_date, exportDb, setting):
    cur = conn.cursor()
    if (exportDb):
        if (setting):
            limits = format_settings(setting)
            query = "SELECT * from beatmaps WHERE approved_date BETWEEN '" + date + "' AND '"+ end_date +"' AND " + limits + " ORDER BY `approved_date` ASC;"
        else:
            query = "SELECT * from beatmaps WHERE approved_date BETWEEN '" + date + "' AND '"+ end_date +"' ORDER BY `approved_date` ASC;"
    else:
        if (setting):
            limits = format_settings(setting)
            query = "SELECT * from beatmaps WHERE " + limits + " ORDER BY `approved_date` ASC;"
        else:
            query = "SELECT * from beatmaps ORDER BY `approved_date` ASC;"
    cur.execute(query)
    return cur.fetchall()

def call_beatmaps_players(conn, date, end_date, exportDb, setting, players):
    cur = conn.cursor()
    select = ""
    columns = ["score_", "accuracy_", "combo_", "date_", "rank_", "pp_", "FC_", "countmiss_", "enabled_mods_"]
    for player in players:
        for precol in columns:
            select = select + "`"+precol+player['username']+"`,"
    select = select[:-1]
    if (exportDb):
        if (setting):
            limits = format_settings(setting)
            query = "SELECT hyperlink,version,max_combo,difficultyrating,artist,title,creator,"+select+" from beatmaps WHERE approved_date BETWEEN '" + date + "' AND '"+ end_date +"' AND " + limits + " ORDER BY `approved_date` ASC;"
        else:
            query = "SELECT hyperlink,version,max_combo,difficultyrating,artist,title,creator,"+select+" from beatmaps WHERE approved_date BETWEEN '" + date + "' AND '"+ end_date +"' ORDER BY `approved_date` ASC;"
    else:
        if (setting):
            limits = format_settings(setting)
            query = "SELECT hyperlink,version,max_combo,difficultyrating,artist,title,creator,"+select+" from beatmaps WHERE " + limits + " ORDER BY `approved_date` ASC;"
        else:
            query = "SELECT hyperlink,version,max_combo,difficultyrating,artist,title,creator,"+select+" from beatmaps ORDER BY `approved_date` ASC;"
    print(query)
    cur.execute(query)
    return cur.fetchall()

def create_json(filename, data):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file)
    return None

def open_json(filename):
    with open(filename) as json_file:
        datos = json.load(json_file)
    return datos

def format_settings(sett):
    query = "difficultyrating BETWEEN "+ str(sett['sr_low']) + " AND "+ str(sett['sr_high']) + " AND diff_approach BETWEEN " + \
            str(sett['ar_low']) + " AND " + str(sett['ar_high']) + " AND diff_size BETWEEN " + str(sett['cs_low']) + " AND " + \
            str(sett['cs_high']) + " AND diff_overall BETWEEN " + str(sett['od_low']) + " AND " + str(sett['od_high']) + \
            " AND bpm BETWEEN " + str(sett["bpm_low"])  + " AND " + str(sett['bpm_high']) + " AND total_length BETWEEN " + \
            str(sett["length_low"])  + " AND " + str(sett['length_high'])
    return query

def translate_mods(num):
    total = int(num)
    lista = []
    string = ""
    for item in reversed(Mod):
        if (item.value <= total):
            total -= item.value
            lista.append(item.name)
            if ((item.name == "DoubleTime" and ("Nightcore" in lista)) or (item.name == "SuddenDeath" and ("Perfect" in lista))):
                pass
            else:
                string = string + item.name + ","
            if (total == 0):
                break
    return string[:-1]


# Funciones threading

def query_api(beatmaps_dict, key, mode, mods):
    for beatmap in beatmaps_dict:
        id = beatmap['beatmap_id']
        query = "https://osu.ppy.sh/api/get_scores?k=" + key + "&b=" + str(id) + "&limit=100&m=" + str(mode) + mods
        score = retry_query(query)
        if (score == []):
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                  + ' - no se encontro score en: ' + str(beatmap['beatmap_id']))
        else:
            user_score_beatmaps_api.put((score,beatmap))
        time.sleep(0.25)

def query_api_to_bd(tipo, lenght, database):
    conn = dbu.create_connection(database)
    cont = contador.get()
    while (user_score_beatmaps_api.qsize() != 0):
        score,beatmap= user_score_beatmaps_api.get()
        id = beatmap['beatmap_id']
        try:
            if (len(score) < 8):
                refresh(beatmap, tipo, [score[0]], conn, id)
            elif (8 <= len(score) < 25):
                refresh(beatmap, tipo, [score[0], score[7]], conn, id)
            elif (25 <= len(score) < 50):
                refresh(beatmap, tipo, [score[0], score[7], score[24]], conn, id)
            elif (50 <= len(score) < 100):
                refresh(beatmap, tipo, [score[0], score[7], score[24], score[49]], conn, id)
            else:
                refresh(beatmap, tipo, [score[0], score[7], score[24], score[49], score[99]], conn, id)
        except:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
            print(sys.exc_info())
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
        cont+=1
        if (cont%500 == 0):
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+" - " + str(cont) + " beatmaps actualizados de "
                  + str(lenght))
            if (cont%20000 == 0):
                conn.close()
                conn = dbu.create_connection(database)
    contador.put(cont)
    conn.close()

def query_user_api(beatmaps_dict, key, mode, usuarios):
    count = 0
    for beatmap in beatmaps_dict:
        start = time.time()
        count+=1
        id = beatmap['beatmap_id']
        for player in usuarios:
            query = "https://osu.ppy.sh/api/get_scores?k=" + key + "&b=" + str(id) + "&m=" + str(mode)+ "&u=" + str(player['id'])
            score = retry_query(query)
            if (score == []):
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+' - no se encontro score en: ' + str(beatmap['hyperlink']) +
                      ' para el player: '+ player['username']+' \t '+ str(count)+'\\'+str(len(beatmaps_dict)))
                pass
            else:
                user_score_beatmaps_api.put((score,beatmap,player))
            time.sleep(0.25)
        end = time.time()
        seconds = end-start
        print("Tiempo de ejecucion para beatmap {:d}/{:d} = {:.2f} seconds".format(count,len(beatmaps_dict),(seconds)))
        print("Tiempo por requests: {:.2f}".format((seconds/len(usuarios))))

def query_user_api_to_bd(database):
    conn = dbu.create_connection(database)
    cont = contador.get()
    while (user_score_beatmaps_api.qsize() != 0):
        score, beatmap, player = user_score_beatmaps_api.get()
        id = beatmap['beatmap_id']
        combo = "NO"
        try:
            if (str(beatmap["max_combo"]) == str(score[0]["maxcombo"])):
                combo = "YES"
        except:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
            print(sys.exc_info())
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
            print("beatmap max_combo: ")
            print(beatmap["max_combo"])
            print("contenido score: ")
            print(score)
            print("score maxcombo: ")
            print(score[0]["maxcombo"])
        accuracy = calc_accuracy(score[0])
        send_dict = {'score_' + player['username']: score[0]['score'],
                    'accuracy_' + player['username']: accuracy,
                    'combo_' + player['username']: score[0]['maxcombo'],
                    'count300_' + player['username']: score[0]['count300'],
                    'count100_' + player['username']: score[0]['count100'],
                    'count50_' + player['username']: score[0]['count50'],
                    'countmiss_' + player['username']: score[0]['countmiss'],
                    'date_' + player['username']: score[0]['date'],
                    'rank_' + player['username']: score[0]['rank'],
                    'pp_' + player['username']: score[0]['pp'],
                    'FC_' + player['username']: combo,
                    'enabled_mods_' + player['username']: translate_mods(score[0]['enabled_mods'])}
        try:
            dbu.update(conn, conn.cursor(), send_dict, id)
        except:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - Error")
            print(sys.exc_info())
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
        cont += 1
        if (cont % 500 == 0):
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " - " + str(cont) + " entradas actualizadas")
            if (cont % 20000 == 0):
                conn.close()
                conn = dbu.create_connection(database)
    contador.put(cont)
    conn.close()

def calc_accuracy(score):
    miss = int(score["countmiss"])
    fifty = int(score["count50"])
    hundred = int(score["count100"])
    threehund = int(score["count300"])
    dividendo = (50*fifty)+(100*hundred)+(300*threehund)
    divisor = (miss+fifty+hundred+threehund)*300
    result = round(((dividendo/divisor)*100),2)
    return result
