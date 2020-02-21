from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from tkinter import ttk
from beatmaps_02 import get_beatmaps, get_scores, open_json, call_beatmaps, create_json
import pandas as pd
import threading, sys, os, json
import db_util as dbu
import time


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
    T = Text(root, wrap=NONE, yscrollcommand=yscrollbar.set)
    sys.stdout = StdoutRedirector(T)
    T.pack()
    T.insert(END, inputStr)
    yscrollbar.config(command=T.yview)

root = Tk()
root.geometry("1000x400")
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
    return '=HYPERLINK("%s", "%s")' % (link, "Link")

def execution(modo, api, name, nomod, top, converts, json, folder, db, update, hyperlink):
    redict = redirector()
    conn = None
    if (db == ""):
        conn = dbu.create_connection(name + ".db")
    else:
        conn = dbu.create_connection(db)
    if (update == False):
        get_beatmaps(modo, api, converts, conn)
    if (top):
        get_scores(api, modo, False, conn)
    if (nomod):
        get_scores(api, modo, True, conn)
    beatmaps = call_beatmaps(conn)
    df = pd.DataFrame.from_records(beatmaps)
    df = df.rename(columns={'approved':'rank_status', 'file_md5': 'hash', 'diff_size': 'CS', 'diff_overall': 'OD',
                       'diff_approach': 'AR', 'diff_drain': 'HP'})
    df = df.replace({'rank_status': {'4':'loved', '3':'qualified', '2':'approved', '1':'ranked','0':'pending', '-1':'WIP',
                                '-2':'graveyard'},
                'mode': {0: 'osu!', 1: 'taiko', 2: 'CtB', 3: 'osu!mania'},
                'genre_id': {0: 'any', 1: 'unspecified', 2: 'video game', 3: 'anime', 4: 'rock', 5: 'pop',
                             6: 'other', 7: 'novelty', 9: 'hip hop', 10: 'electronic'},
                'language_id': {0: 'any', 1: 'other', 2: 'english', 3: 'japanese', 4: 'chinese',
                                5: 'instrumental', 6: 'korean', 7: 'french', 8: 'german', 9: 'swedish',
                                10: 'spanish', 11: 'italian'}
                })
    for col in df.columns:
        df[col] = df[col].map(lambda x: str(x).replace('=', "__"))
    if (hyperlink):
        df["hyperlink"] = df.apply(lambda row: mode_label(row), axis=1)
        columnas = list(df.columns)
        df = df[columnas[:1]+columnas[-1:]+columnas[1:-1]]
    print("Generando Excel")
    df.to_excel(folder + '/' + name.replace(".", "") + ".xlsx", sheet_name='Sheet1', index=False, engine='openpyxl')
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
    converts = converted.get()
    db = database.get()
    update = updateDB.get()
    link = hyperlink.get()
    if (newDB.get() == True):
        db = ""
    if (name == "" or folder == "" or api == ""):
        messagebox.showinfo("Faltan datos", "Faltan datos por completar")
    else:
        try:
            dir = os.getcwd() + '\\api.json'
            data = {}
            data['api'] = api
            with open(dir, 'w') as json_file:
                json.dump(data, json_file)
            thread = threading.Thread(target=execution, args=(modo, api, name, nomod, top, converts, json_value, folder, db, update, link))
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
newDBbox = ttk.Checkbutton(root, text= "Nueva Base de datos (se creara en base al nombre del archivo)", variable=newDB)
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
hyperlink = BooleanVar()
hyperBox = ttk.Checkbutton(root, text="Generate beatmaps links", variable=hyperlink)
hyperBox.grid(row=9,column=0, sticky=E, pady=10)

    #Buttons
sendButton = Button(root, text= "Start!", state=ACTIVE, padx=50, pady=1, command=myClick)
sendButton.grid(row=10,column=1, pady=25)
button_quit = Button(root, text= "Exit", command=root.quit, width=10).grid(row=12,column=5, sticky=W+E)


root.mainloop()