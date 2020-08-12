from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from tkinter import ttk
from beatmaps_02 import get_beatmaps, get_scores, open_json, call_beatmaps, create_json, validating_user, user_scores, call_beatmaps_players
import pandas as pd
import threading, sys, os, json
import db_util as dbu
import datetime


class IORedirector(object):
    '''A general class for redirecting I/O to this Text widget.'''
    def __init__(self,text_area):
        self.text_area = text_area

class StdoutRedirector(IORedirector):
    '''A class for redirecting stdout to this Text widget.'''
    def write(self,str):
        self.text_area.insert("end", str)
        self.text_area.see("end")
    def flush(self):
        pass

def redirector(inputStr=""):
    root = Toplevel()
    yscrollbar = Scrollbar(root)
    yscrollbar.pack(side=RIGHT, fill=Y)
    T = Text(root, wrap=NONE, yscrollcommand=yscrollbar.set,bg='#121212', fg='#FFFFFF')
    sys.stdout = StdoutRedirector(T)
    T.pack(expand=YES, fill=BOTH)
    T.insert(END, inputStr)
    yscrollbar.config(command=T.yview)

root = Tk()
root.geometry("1200x720")
root.iconbitmap('3172980.ico')
root.title("osuScores")
root.grid_rowconfigure(15, weight=1)
root.grid_columnconfigure(10, weight=1)

def mode_label(row):
    link = "https://osu.ppy.sh/beatmapsets/" + row["beatmapset_id"] + "#"+row["mode"]+"/" + row["beatmap_id"]
    if row['mode'] == "CtB":
        link = "https://osu.ppy.sh/beatmapsets/" + row["beatmapset_id"] + "#fruits/" + row["beatmap_id"]
    if row['mode'] == "osu!mania":
        link = "https://osu.ppy.sh/beatmapsets/" + row["beatmapset_id"] + "#mania/" + row["beatmap_id"]
    #return '=HYPERLINK("%s", "%s")' % (link, "Link")
    return link

def execution(modo, api, name, nomod, top, converts, json, folder, db, update, fecha, fecha_end, exported_db, player, p_confirm, only_user, settings, loved):
    redict = redirector()
    conn = None
    if (db == ""):
        conn = dbu.create_connection(name + ".db")
        db = name + ".db"
    else:
        conn = dbu.create_connection(db)
    if (update == False):
        get_beatmaps(modo, api, converts, conn, fecha, fecha_end, loved)
        conn.close()
    # Connections are opened and closed for every function, because of timeout problems, some executions could take 3+ days
    if (top):
        print("Minando Top scores")
        get_scores(api, modo, False, fecha, str(fecha_end), settings, db)
    if (nomod):
        print("Minando No mod scores")
        get_scores(api, modo, True, fecha, str(fecha_end), settings, db)
    if (p_confirm):
        print("Minando scores de usuarios")
        user_scores(api, fecha, str(fecha_end), player, modo, settings, db)
    conn = dbu.create_connection(db)
    if (only_user):
        beatmaps = call_beatmaps_players(conn, fecha, str(fecha_end), exported_db, settings, player)
        df = pd.DataFrame.from_records(beatmaps)
    else:
        beatmaps = call_beatmaps(conn, fecha, str(fecha_end), exported_db, settings)
        df = pd.DataFrame.from_records(beatmaps)
        df = df.rename(columns={'approved': 'rank_status', 'file_md5': 'hash', 'diff_size': 'CS', 'diff_overall': 'OD',
                                'diff_approach': 'AR', 'diff_drain': 'HP'})
        df = df.replace({'rank_status': {'4': 'loved', '3': 'qualified', '2': 'approved', '1': 'ranked', '0': 'pending',
                                         '-1': 'WIP',
                                         '-2': 'graveyard'},
                         'mode': {0: 'osu!', 1: 'taiko', 2: 'CtB', 3: 'osu!mania'},
                         'genre_id': {0: 'any', 1: 'unspecified', 2: 'video game', 3: 'anime', 4: 'rock', 5: 'pop',
                                      6: 'other', 7: 'novelty', 9: 'hip hop', 10: 'electronic'},
                         'language_id': {0: 'any', 1: 'other', 2: 'english', 3: 'japanese', 4: 'chinese',
                                         5: 'instrumental', 6: 'korean', 7: 'french', 8: 'german', 9: 'swedish',
                                         10: 'spanish', 11: 'italian'}
                         })
    conn.close()
    for col in df.columns:
        df[col] = df[col].map(lambda x: str(x).replace('=', "__"))
    print("Generando Excel")
    #df.to_excel(folder + '/' + name.replace(".", "") + ".xlsx", sheet_name='Sheet1', index=False, engine='openpyxl')
    df.to_csv(folder + '/' + name.replace(".", "") + ".csv", index=False, encoding='utf-8')
    if (json):
        print("Generando json")
        df.to_json(folder + '/' + name.replace(".", "") + ".json")
    messagebox.showinfo("Exito", "Se ha realizado la operacion con exito")
    sendButton.config(state=ACTIVE)

def myClick():
    sendButton.config(state=DISABLED)
    name = filename.get()
    api = entryBox.get()
    folder = folder_path.get()
    modo = selection.get()
    top = top_score.get()
    nomod = top_nomod.get()
    json_value = create_json.get()
    loved_maps = loved.get()
    converts = converted.get()
    db = database.get()
    update = updateDB.get()
    day = entryDay.get()
    mes = entryMonth.get()
    year = entryYear.get()
    end_day = entryDay_end.get()
    end_mes = entryMonth_end.get()
    end_year = entryYear_end.get()
    fecha = '2000-02-18 17:46:31'
    fecha_end = datetime.datetime.today()
    entire_db = export_db.get()
    user = entryUser.get()
    confirm = user_confirm.get()
    only_user = onlyUser_confirm.get()
    limits = {}
    if (confirm or only_user):
        player = validating_user(user, api)
    else:
        player = [{'id': '', 'username': ''}]
    if (settings.get()):
        try:
            sr_low = float(str(entry_sr_low.get()).replace(",","."))
            sr_high = float(str(entry_sr_high.get()).replace(",","."))
            ar_low = float(str(entry_ar_low.get()).replace(",","."))
            ar_high = float(str(entry_ar_high.get()).replace(",","."))
            cs_low = float(str(entry_cs_low.get()).replace(",","."))
            cs_high = float(str(entry_cs_high.get()).replace(",","."))
            od_low = float(str(entry_od_low.get()).replace(",","."))
            od_high = float(str(entry_od_high.get()).replace(",","."))
            bpm_low = float(str(entry_bpm_low.get()).replace(",","."))
            bpm_high = float(str(entry_bpm_high.get()).replace(",","."))
            length_low = float(str(entry_length_low.get()).replace(",","."))
            length_high = float(str(entry_length_high.get()).replace(",","."))
            if ((sr_low <= sr_high) and (ar_low <= ar_high) and (cs_low <= cs_high) and (od_low <= od_high) and (bpm_low <= bpm_high)):
                limits.update({'sr_low':sr_low,'sr_high':sr_high,'ar_low':ar_low,'ar_high':ar_high,'cs_low':cs_low,
                               'cs_high':cs_high, 'od_low':od_low, 'od_high':od_high, 'bpm_low':bpm_low,
                               'bpm_high': bpm_high, 'length_low': length_low, 'length_high': length_high})
            else:
                messagebox.showinfo("Datos Incongruentes", "Los datos ingresados no tienen sentido, desconchetumarizate!")
                sendButton.config(state=ACTIVE)
                root.quit()
        except:
            pass
    try:
        int(day)
        int(mes)
        int(year)
        int(end_day)
        int(end_mes)
        int(end_year)
        if (int(mes) == 2 and int(day) > 28):
            day = '28'
        if (int(end_mes) == 2 and int(end_day) > 28):
            end_day = '28'
        date = year+'-'+mes+'-'+day
        fecha = str(datetime.datetime.strptime(date, '%Y-%m-%d'))
        fecha_end = datetime.datetime(int(end_year),int(end_mes),int(end_day),23,59,59)
    except:
        pass
    if (newDB.get() == True):
        db = ""
    if (name == "" or folder == "" or api == ""):
        messagebox.showinfo("Faltan datos", "Faltan datos por completar")
        sendButton.config(state=ACTIVE)
    elif(not bool(player) and confirm):
        messagebox.showinfo("BAD_USERNAME", "El player ingresado no existe o ha sido mal ingresado")
        sendButton.config(state=ACTIVE)
    elif(settings.get() and not bool(limits)):
        messagebox.showinfo("NOT_FLOAT", "Se tienen que ingresar numeros o decimales")
        sendButton.config(state=ACTIVE)
    else:
        try:
            dir = os.getcwd() + '\\api.json'
            data = {}
            data['api'] = api
            with open(dir, 'w') as json_file:
                json.dump(data, json_file)
            thread = threading.Thread(target=execution, args=(modo, api, name, nomod, top, converts, json_value, folder, db, update, fecha, fecha_end, entire_db, player, confirm, only_user, limits, loved_maps))
            thread.start()
        except Exception as e:
            messagebox.showinfo("Error", "Wrong API key!!!!!")
            root.quit()

def browse_button():
    # Allow user to select a directory and store it in global var
    # called folder_path
    global folder_path
    filename = filedialog.askdirectory()
    folder_path.set(filename)

def turn_off():
    convertedBox.config(state=DISABLED)
    converted.set(False)

def turn_on():
    convertedBox.config(state=ACTIVE)

def disable_db():
    if(newDB.get()):
        updateBox.config(state=DISABLED)
    else:
        updateBox.config(state=ACTIVE)

api_key = ""
try:
    api_key = open_json("api.json")["api"]
except:
    pass
# Labels & Entries
    #Filename Block
file_label = Label(root, text="Ingrese el nombre del archivo: ", width=25).grid(row=0,column=0)
filename = Entry(root)
filename.grid(row=0,column=1, columnspan=2, sticky=W+E, pady=5)

    #API Block
api_key_label = Label(root, text="Ingrese API key:", width=25).grid(row=1,column=0)
entryBox = Entry(root)
entryBox.insert(0, api_key)
entryBox.grid(row=1,column=1, columnspan=2, sticky=W+E, pady=5)

    #SearchFolder Block
folder_path = StringVar()
folderLabel = Label(root,text="Seleccione Carpeta:", width=25).grid(row=2,column=0)
folder_label = Label(root, textvariable=folder_path, bg='white').grid(row=2,column=1, columnspan=2, sticky=W+E)
folder_button = Button(text="Browse", command=browse_button).grid(row=2,column=3,sticky=W, pady=5)

    #Database Block
options = dbu.get_db_names()
database = StringVar()
database_label = Label(root, text="Base de Datos: ").grid(row=3,column=0)
updateDB = BooleanVar()
updateBox = ttk.Checkbutton(root, text= "Actualizar BD", variable=updateDB)
updateBox.grid(row=3, column=2)
if options != []:
    database.set(options[0])
    database_menu = OptionMenu(root, database, *options).grid(row=3, column=1)
else:
    white_label = Label(root, text="(No se han encontrado bases de datos)").grid(row=3, column=1)
    updateBox.config(state=DISABLED)
newDB = BooleanVar()
newDBbox = ttk.Checkbutton(root, text= "Nueva Base de datos (se creara en base al nombre del archivo)", variable=newDB, command=disable_db)
newDBbox.grid(row=3, column=3)

    #Select GameMode Block
selection = IntVar()
radio_label = Label(root, text="Seleccione modo de juego:").grid(row=4, column=0)
Radiobutton(root, text="osu!", variable=selection, value=0, anchor=W, command=turn_off).grid(row=4,column=1,sticky=W+E)
Radiobutton(root, text="taiko", variable=selection, value=1, anchor=W, command=turn_on).grid(row=5,column=1,sticky=W+E)
Radiobutton(root, text="CtB", variable=selection, value=2, anchor=W, command=turn_on).grid(row=6,column=1,sticky=W+E)
Radiobutton(root, text="osu!mania", variable=selection, value=3, anchor=W, command=turn_on).grid(row=7,column=1,sticky=W+E)

    #Checkboxes
top_score = BooleanVar()
topScoreBox = ttk.Checkbutton(root, text="Top Score", variable=top_score).grid(row=8,column=2)
top_nomod = BooleanVar()
topNoModBox = ttk.Checkbutton(root, text="Nomod Top Score", variable= top_nomod).grid(row=8,column=3)
create_json = BooleanVar()
jsonBox = ttk.Checkbutton(root, text = "Generate .json File", variable=create_json).grid(row=8,column=0)
converted = BooleanVar()
convertedBox = ttk.Checkbutton(root, text= "Include Converted Beatmaps", variable=converted, state="disabled")
convertedBox.grid(row=8,column=1)
loved = BooleanVar()
lovedBox = ttk.Checkbutton(root, text = "Add loved maps", variable=loved).grid(row=8,column=4)


    #Settings
Label(root, text="Settings: ").grid(row=9, column=0)
settings_frame = Frame(root)
settings_frame.grid(row=9, column=1, columnspan=4, pady=20)
    #Star Rating
Label(settings_frame, text="Star rating: ").grid(row=0,column=0,padx=3)
entry_sr_low = Entry(settings_frame, width=4)
entry_sr_low.grid(row=0, column=1, padx=3)
entry_sr_low.insert(0,"0.0")
entry_sr_high = Entry(settings_frame, width=4)
entry_sr_high.grid(row=0,column=2,padx=3)
entry_sr_high.insert(0,"10.0")
    #Approach Rate
Label(settings_frame, text="AR: ").grid(row=0,column=3,padx=3)
entry_ar_low = Entry(settings_frame, width=4)
entry_ar_low.grid(row=0, column=4,padx=3)
entry_ar_low.insert(0,"0.0")
entry_ar_high = Entry(settings_frame, width=4)
entry_ar_high.grid(row=0,column=5,padx=3)
entry_ar_high.insert(0,"10.0")
    #Circle Size
Label(settings_frame, text="CS: ").grid(row=0,column=6,padx=3)
entry_cs_low = Entry(settings_frame, width=4)
entry_cs_low.grid(row=0, column=7,padx=3)
entry_cs_low.insert(0,"0.0")
entry_cs_high = Entry(settings_frame, width=4)
entry_cs_high.grid(row=0,column=8,padx=3)
entry_cs_high.insert(0,"7.0")
    #Overall Diff
Label(settings_frame, text="OD: ").grid(row=0,column=9,padx=3)
entry_od_low = Entry(settings_frame, width=4)
entry_od_low.grid(row=0, column=10,padx=3)
entry_od_low.insert(0,"0.0")
entry_od_high = Entry(settings_frame, width=4)
entry_od_high.grid(row=0,column=11,padx=3)
entry_od_high.insert(0,"10.0")
    #BPM
Label(settings_frame, text="BPM: ").grid(row=0,column=12,padx=3)
entry_bpm_low = Entry(settings_frame, width=4)
entry_bpm_low.grid(row=0, column=13,padx=3)
entry_bpm_low.insert(0,"0")
entry_bpm_high = Entry(settings_frame, width=4)
entry_bpm_high.grid(row=0,column=14,padx=3)
entry_bpm_high.insert(0,"300")
    #LENGTH
Label(settings_frame, text="Length: ").grid(row=0,column=15,padx=3)
entry_length_low = Entry(settings_frame, width=4)
entry_length_low.grid(row=0, column=16,padx=3)
entry_length_low.insert(0,"0")
entry_length_high = Entry(settings_frame, width=6)
entry_length_high.grid(row=0,column=17,padx=6)
entry_length_high.insert(0,"99999")
    #Checkbox
settings = BooleanVar()
settingBox = ttk.Checkbutton(settings_frame, text="Apply Settings", variable=settings)
settingBox.grid(row=0,column=18,padx=40)

    #Datepicker
date_label = Label(root, text="Fecha inicial: ").grid(row=10, column=0)
frame = Frame(root)
frame.grid(row=10, column=1, sticky="nsew")
entryYear = Entry(frame,width=5)
entryYear.grid(row=0,column=0)
entryYear.insert(0,"year")
Label(frame, text="/").grid(row=0,column=1)
entryMonth = Entry(frame,width=4)
entryMonth.grid(row=0,column=3)
entryMonth.insert(0,"mm")
Label(frame, text="/").grid(row=0,column=4)
entryDay = Entry(frame,width=4)
entryDay.grid(row=0,column=5)
entryDay.insert(0,"dd")

date_end_label = Label(root, text="Fecha final: ").grid(row=10, column=2)
today_datetime = datetime.datetime.today() + datetime.timedelta(days=1)
frame_end = Frame(root)
frame_end.grid(row=10, column=3, sticky="nsew")
entryYear_end = Entry(frame_end,width=5)
entryYear_end.grid(row=0,column=0)
entryYear_end.insert(0,today_datetime.year)
Label(frame_end, text="/").grid(row=0,column=1)
entryMonth_end = Entry(frame_end,width=4)
entryMonth_end.grid(row=0,column=3)
entryMonth_end.insert(0,today_datetime.month)
Label(frame_end, text="/").grid(row=0,column=4)
entryDay_end = Entry(frame_end,width=4)
entryDay_end.grid(row=0,column=5)
entryDay_end.insert(0,today_datetime.day)

    #Date checkbox
export_db = BooleanVar()
exportBox = ttk.Checkbutton(root, text="Generar csv desde esta fecha?", variable=export_db)
exportBox.grid(row=10,column=4, sticky=W)
#slash_label = Label(root, text="/").grid(row=10,column=1, sticky=W)

    #User Block
user_label = Label(root, text="Usuario a seguir?(nick or id): ").grid(row=11, column=0, pady= 20)
entryUser = Entry(root)
entryUser.grid(row=11,column=1,sticky=W+E)
user_confirm = BooleanVar()
userBox = ttk.Checkbutton(root, text="Considerar usuario?", variable=user_confirm)
userBox.grid(row=11,column=2)
onlyUser_confirm = BooleanVar()
onlyUserBox = ttk.Checkbutton(root, text="Solo data de usuarios?", variable=onlyUser_confirm)
onlyUserBox.grid(row=11,column=3, sticky=W)

    #Buttons
sendButton = Button(root, text= "Start!", state=ACTIVE, padx=50, pady=1, command=myClick)
sendButton.grid(row=12,column=1, pady=25)
button_quit = Button(root, text= "Exit", command=root.quit, width=10).grid(row=12,column=5, sticky=W+E)


root.mainloop()