from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from tkinter import ttk
from beatmaps_02 import get_beatmaps, get_scores
import pandas as pd
import threading

class IORedirector(object):
    '''A general class for redirecting I/O to this Text widget.'''
    def __init__(self,text_area):
        self.text_area = text_area

class StdoutRedirector(IORedirector):
    '''A class for redirecting stdout to this Text widget.'''
    def write(self,str):
        self.text_area.insert("end", str)
    def flush(self):
        pass

def redirector(inputStr=""):
    import sys
    root = Toplevel()
    T = Text(root)
    sys.stdout = StdoutRedirector(T)
    T.pack()
    T.insert(END, inputStr)

root = Tk()
root.geometry("700x400")
root.iconbitmap('3172980.ico')
root.title("osuScores")
root.grid_rowconfigure(10, weight=1)
root.grid_columnconfigure(10, weight=1)


def execution(modo, api, name, nomod, top, converts, json, folder):
    redict = redirector()
    beatmaps = get_beatmaps(modo, api, name.replace(".", ""), (nomod or top), converts, json, folder)
    if (top):
        beatmaps = get_scores(beatmaps, api, modo, json, name.replace(".", ""), False, folder)
    if (nomod):
        beatmaps = get_scores(beatmaps, api, modo, json, name.replace(".", ""), True, folder)
    df = pd.DataFrame.from_records(beatmaps)
    for col in df.columns:
        df[col] = df[col].map(lambda x: str(x).replace('=', "__"))
    df.to_excel(folder + '/' + name.replace(".", "") + ".xlsx", sheet_name='Sheet1', index=False, engine='openpyxl')
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
    json = create_json.get()
    converts = converted.get()
    if (name == "" or folder == "" or api == ""):
        messagebox.showinfo("Faltan datos", "Faltan datos por completar")
    else:
        try:
            thread = threading.Thread(target=execution, args=(modo, api, name, nomod, top, converts, json, folder))
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

# Labels & Entries
    #Filename Block
file_label = Label(root, text="Ingrese el nombre del archivo: ", width=25).grid(row=0,column=0)
filename = Entry(root)
filename.grid(row=0,column=1, columnspan=2, sticky=W+E, pady=5)

    #API Block
api_key_label = Label(root, text="Ingrese API key:", width=25).grid(row=1,column=0)
entryBox = Entry(root)
entryBox.grid(row=1,column=1, columnspan=2, sticky=W+E, pady=5)

    #SearchFolder Block
folder_path = StringVar()
folderLabel = Label(root,text="Seleccione Carpeta:", width=25).grid(row=2,column=0)
folder_label = Label(root, textvariable=folder_path, bg='white').grid(row=2,column=1, columnspan=2, sticky=W+E)
folder_button = Button(text="Browse", command=browse_button).grid(row=2,column=3,sticky=W, pady=5)

    #Select GameMode Block
selection = IntVar()
radio_label = Label(root, text="Seleccione modo de juego:").grid(row=3, column=0)
Radiobutton(root, text="osu!", variable=selection, value=0, anchor=W, command=turn_off).grid(row=3,column=1,sticky=W+E)
Radiobutton(root, text="taiko", variable=selection, value=1, anchor=W, command=turn_on).grid(row=4,column=1,sticky=W+E)
Radiobutton(root, text="CtB", variable=selection, value=2, anchor=W, command=turn_on).grid(row=5,column=1,sticky=W+E)
Radiobutton(root, text="osu!mania", variable=selection, value=3, anchor=W, command=turn_on).grid(row=6,column=1,sticky=W+E)

    #Checkboxes
top_score = BooleanVar()
topScoreBox = ttk.Checkbutton(root, text="Top Score", variable=top_score).grid(row=7,column=2)
top_nomod = BooleanVar()
topNoModBox = ttk.Checkbutton(root, text="Nomod Top Score", variable= top_nomod).grid(row=7,column=3)
create_json = BooleanVar()
jsonBox = ttk.Checkbutton(root, text = "Generate .json File", variable=create_json).grid(row=7,column=0)
converted = BooleanVar()
convertedBox = ttk.Checkbutton(root, text= "Include Converted Beatmaps", variable=converted, state="disabled")
convertedBox.grid(row=7,column=1)

    #Buttons
sendButton = Button(root, text= "Start!", state=ACTIVE, padx=50, pady=1, command=myClick)
sendButton.grid(row=8,column=1, pady=25)
button_quit = Button(root, text= "Exit", command=root.quit, width=10).grid(row=10,column=5, sticky=W+E)


root.mainloop()