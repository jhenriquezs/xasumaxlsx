import json, requests, time, sys

def get_beatmaps(mode, key, filename, mods, conv, json_value, folder_path):
    base_date = '2020-01-31 17:46:31'
    converted = ""
    if (conv):
        converted = '&a=1'
    try:
        data = requests.get("https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ base_date +"&m="+str(mode)+"&limit=500"+converted).json()
    except:
        sys.exit("Your api key is wrong!!!!!")
    beatmaps_total = data

    while (len(data)!= 0):
        if (len(data) < 500):
            break
        print("beatmaps hasta el momento: " + str(len(beatmaps_total)))
        last_map = data[-1]
        time.sleep(1)
        data = requests.get("https://osu.ppy.sh/api/get_beatmaps?k="+ key +"&since="+ last_map['approved_date'] +"&m="+str(mode)+"&limit=500"+converted).json()
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
                cont+=1
                fecha = data[cont]['approved_date']
        else:
            beatmaps_total= beatmaps_total + data
    if (json_value and (mods == False)):
        create_json(beatmaps_total, folder_path+'/'+filename+'.json')
    print("beatmaps en total: "+ str(len(beatmaps_total)))
    return beatmaps_total

def get_scores(beatmaps_dict, key, mode, json_value, filename, no_mod, folder_path):
    tipo = "Top_"
    mods = ""
    if (no_mod):
        mods = "&mods=(0|16384)"
        tipo = "No_Mod_"
    for beatmap in beatmaps_dict:
        time.sleep(1)
        id = beatmap['beatmap_id']
        query = "https://osu.ppy.sh/api/get_scores?k="+ key +"&b="+ str(id) +"&limit=100&m=" + str(mode) + mods
        score = requests.get(query).json()
        if (score == []):
            print('no se encontro score')
            pass
        else:
            if (len(score) < 8):
                refresh(beatmap, tipo, [score[0]])
                #beatmap.update({tipo+'score': score[0]["score"], tipo+'username': score[0]["username"], tipo+'user_id': score[0]["user_id"],
                #                tipo+'maxcombo': score[0]["maxcombo"], tipo+'date': score[0]["date"], tipo+'rank': score[0]["rank"]})
            elif (8 <= len(score) < 25):
                refresh(beatmap, tipo ,[score[0], score[7]])
            elif (25 <= len(score) < 50):
                refresh(beatmap, tipo ,[score[0], score[7], score[24]])
            elif (50 <= len(score) < 100):
                refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49]])
            else:
                refresh(beatmap, tipo ,[score[0], score[7], score[24], score[49], score[99]])
        if ((beatmaps_dict.index(beatmap)+1)%500 == 0):
            print("500 beatmaps se han actualizado")
    if (json_value):
        create_json(beatmaps_dict, folder_path +'/'+filename+'.json')
    return beatmaps_dict

def refresh(beatmap, tipo ,scores):
    if (tipo == "No_Mod_"):
        beatmap.update({tipo + 'score': scores[0]["score"], tipo + 'username': scores[0]["username"],
                        tipo + 'user_id': scores[0]["user_id"],
                        tipo + 'maxcombo': scores[0]["maxcombo"], tipo + 'date': scores[0]["date"],
                        tipo + 'rank': scores[0]["rank"]})
    else:
        rank = ["","8","25","50","100"]
        contador = 0
        for puntuacion in scores:
            combo = "NO"
            if (beatmap["max_combo"] == puntuacion["maxcombo"]):
                combo = "YES"
            beatmap.update({tipo + rank[contador] + 'score': puntuacion["score"], tipo + rank[contador] +'username': puntuacion["username"],
                            tipo + rank[contador] +'user_id': puntuacion["user_id"],
                            tipo + rank[contador] +'maxcombo': puntuacion["maxcombo"], tipo + rank[contador] +'date': puntuacion["date"],
                            tipo + rank[contador] +'rank': puntuacion["rank"], tipo + rank[contador] +'pp': puntuacion["pp"],
                            tipo + rank[contador] + 'FC?': combo})
            contador+=1

def create_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file)

def open_json(filename):
    with open(filename) as json_file:
        datos = json.load(json_file)
    return datos
