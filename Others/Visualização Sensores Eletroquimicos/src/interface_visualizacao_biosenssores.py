from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.filechooser import FileChooserListView
from kivy.uix.popup import Popup
from kivy.uix.image import Image
from kivy.uix.textinput import TextInput
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import csv
import itertools
from sklearn.manifold import TSNE, MDS

def generate_plot(result_df, n_arquivos):

    # Generate colors for each folder
    colors = itertools.cycle(plt.cm.get_cmap('tab10').colors)

    # Step 1: Determine the minimum shape across all DataFrames
    min_shape = min([df.shape for df in result_df])

    # Step 2: Truncate or pad each DataFrame to match the minimum shape
    standardized_data = []
    for df in result_df:
        flattened = df.to_numpy().flatten()

        # Truncate if longer
        if len(flattened) > min_shape[0] * min_shape[1]:
            standardized_data.append(flattened[:min_shape[0] * min_shape[1]])
        # Pad if shorter
        else:
            padding = np.zeros(min_shape[0] * min_shape[1] - len(flattened))
            standardized_data.append(np.concatenate([flattened, padding]))

    # Step 3: Convert the standardized list into a feature matrix
    feature_matrix = np.array(standardized_data)

    # Step 4: Set perplexity for t-SNE (it must be less than the number of samples)
    n_samples = feature_matrix.shape[0]
    perplexity = min(30, n_samples - 1)  # Adjust perplexity if necessary

    # Step 5: Apply t-SNE
    tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity)
    tsne_result = tsne.fit_transform(feature_matrix)

    # Step 6: Apply MDS
    mds = MDS(n_components=2, random_state=42)
    mds_result = mds.fit_transform(feature_matrix)

    # Step 7: Plot the results with color coding by folder
    plt.figure(figsize=(12, 6))

    # t-SNE plot
    plt.subplot(1, 2, 1)
    start = 0
    for count, color in zip(n_arquivos, colors):
        end = start + count
        plt.scatter(tsne_result[start:end, 0], tsne_result[start:end, 1], c=[color], label=f'Pasta {start//count + 1}')
        start = end
    plt.title('Visualização t-SNE')
    plt.xlabel('Componente 1')
    plt.ylabel('Componente 2')
    plt.legend()

    # MDS plot
    plt.subplot(1, 2, 2)
    start = 0
    for count, color in zip(n_arquivos, colors):
        end = start + count
        plt.scatter(mds_result[start:end, 0], mds_result[start:end, 1], c=[color], label=f'Pasta {start//count + 1}')
        start = end
    plt.title('Visualização MDS')
    plt.xlabel('Componente 1')
    plt.ylabel('Componente 2')
    plt.legend()

    plt.tight_layout()
    plt.savefig('/storage/emulated/0/tsne_mds_plot.png', bbox_inches='tight')  # Save plot to file
    plt.close()

class FileChooserApp(App):
    def build(self):

        self.layout = BoxLayout(orientation='vertical')


        # Create a button to open the file chooser
        self.select_folder_btn = Button(text='Selecione diretório', size_hint_y=None, height=50)
        self.select_folder_btn.bind(on_press=self.open_filechooser)

        # Create a button to execute process_data
        self.process_data_btn = Button(text='Atualiza gráfico', size_hint_y=None, height=50)
        self.process_data_btn.bind(on_press=self.process_data)

        # Create a button to input an integer
        #self.input_int_btn = Button(text='Input Integer', size_hint_y=None, height=50)
        #self.input_int_btn.bind(on_press=self.open_input_popup)

        # Add the buttons to a horizontal layout
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=50)
        button_layout.add_widget(self.select_folder_btn)
        button_layout.add_widget(self.process_data_btn)
        #button_layout.add_widget(self.input_int_btn)

        self.layout.add_widget(button_layout)

        # Label to display selected folder
        self.folder_label = Label(text='No folder selected', size_hint_y=None, height=50)
        self.layout.add_widget(self.folder_label)

        # Variable to store the selected folder path
        self.selected_folder_path = ''

        # Variable to store the integer input
        self.user_input_number = None

        return self.layout

    def open_filechooser(self, instance):
        # Create a file chooser for selecting folders
        self.filechooser = FileChooserListView(dirselect=True)
        self.filechooser.path = '/storage/emulated/0/'  # Use an accessible path

        # Create a button to confirm the selection
        select_button = Button(text='Select', size_hint_y=None, height=50)
        select_button.bind(on_press=self.on_folder_selected)

        # Create a layout to hold the file chooser and the select button
        chooser_layout = BoxLayout(orientation='vertical')
        chooser_layout.add_widget(self.filechooser)
        chooser_layout.add_widget(select_button)

        # Add the file chooser to a popup and open it
        self.popup = Popup(title='Select a Folder', content=chooser_layout, size_hint=(0.9, 0.9), auto_dismiss=False)
        self.popup.open()

    def on_folder_selected(self, instance):
        # Get the selected folder path
        selection = self.filechooser.selection
        if selection:
            # Update the selected folder path variable
            self.selected_folder_path = selection[0]

            # Update label with the selected folder path
            self.folder_label.text = f'Selected Folder: {self.selected_folder_path}'
            # Close the popup after selection
            self.popup.dismiss()

    # Essa função cria uma lista final_list com todos os
    # endereços das subpastas de determinada pasta path
    def set_directories(self, path):
        self.subfolders = [f.name for f in os.scandir(path) if f.is_dir()]
        self.final_list = []
        for i in self.subfolders:
            self.final_list.append(path + r'/' + i + r'/')


    # Retorna uma lista que mostra todos os arquivos em determinado diretório, junto da extensão.
    def list_files_in_directory(self, directory):
        # Get list of files in the directory
        self.files = os.listdir(directory)
        self.arquivos = []
        for file in self.files:
            self.arquivos.append(file)
        return self.arquivos

    def contar_arquivos_em_subpastas(self,diretorio):
        # Lista para armazenar o número de arquivos em cada subpasta
        self.numero_arquivos = []

        # Itera por todas as subpastas do diretório especificado
        for root, dirs, files in os.walk(diretorio):
            # Conta o número de arquivos na subpasta atual
            self.numero_arquivos.append(len(files))

        # Remove o primeiro valor que corresponde ao diretório raiz
        if len(self.numero_arquivos) > 0:
            self.numero_arquivos.pop(0)

        return self.numero_arquivos

    def process_data(self, instance):
        # 1º - Selecionar a pasta onde estão as subpastas da medida
        self.set_directories(self.selected_folder_path)

        # Processa e transforma os dados em dataframe do Pandas
        self.pandificador(self.final_list)

        # Remove todos os widgets da área do gráfico
        for widget in self.layout.children[:]:
            if isinstance(widget, Image):
                self.layout.remove_widget(widget)

        # Gera os plots
        n_arq = self.contar_arquivos_em_subpastas(self.selected_folder_path)
        generate_plot(self.result_df, n_arq)  # Salva o novo gráfico

        # Adiciona o novo gráfico à interface
        self.plot_image = Image(source='/storage/emulated/0/tsne_mds_plot.png', size_hint=(1, 0.75))
        self.layout.add_widget(self.plot_image)

    def process_csv(self, input_file, output_file, search_string):
        self.remove_first_5_lines(input_file)

        with open(input_file, 'r', newline='', encoding='UTF-16') as fin:
            reader = csv.reader(fin)
            rows = list(reader)

        # Não sei se precisa mais disso, mas não me atrevo a remover.
        for row in rows:
            if row:  # Check if row is not empty
                for i, item in enumerate(row):
                    # Replace "à¨à´€" with comma
                    item = item.replace("à¨à´€", ",")

                    # Update the row with modified item
                    row[i] = item

        # Write the modified content to a new CSV file
        with open(output_file, 'w', newline='', encoding='UTF-16') as fout:
            writer = csv.writer(fout)
            writer.writerows(rows)

    def remove_first_5_lines(self, input_file):
        with open(input_file, 'r', encoding='utf-16') as file:
            self.lines = file.readlines()[5:]

        with open(input_file, 'w', encoding='utf-16', newline='') as file:
            file.writelines(self.lines)

    def pandificador(self, lista_diretorios):
        self.result_df = []
        i = 0
        j = 0

        # Itera sobre todas as pastas
        while i < len(lista_diretorios):

            # Pega o nome dos arquivos de uma subpasta específica
            nome_arquivos = self.list_files_in_directory(lista_diretorios[i])
            j = 0
            # Itera sobre todos os arquivos
            while j < len(nome_arquivos):
                # Endereço completo do arquivo
                endereco_arquivo = lista_diretorios[i] + nome_arquivos[j]
                print("Arquivo localizado em: " + endereco_arquivo)

                # Sanitiza o csv
                self.process_csv(endereco_arquivo, 'output_' + str(i), 'V,')

                df_temp = pd.read_csv(endereco_arquivo, encoding='UTF-16')

                # Remove a última linha do df temporário (linha com NaN)
                df_temp = df_temp.drop(df_temp.index[-1])

                self.result_df.append(df_temp)
                j += 1
            i += 1

    def open_input_popup(self, instance):
        # Create a TextInput for number input
        self.input_text = TextInput(text='', hint_text='Enter an integer', multiline=False, input_filter='int')

        # Create a button to confirm input
        confirm_button = Button(text='Confirm', size_hint_y=None, height=50)
        confirm_button.bind(on_press=self.on_confirm_input)

        # Create a layout for the input and the button
        input_layout = BoxLayout(orientation='vertical')
        input_layout.add_widget(self.input_text)
        input_layout.add_widget(confirm_button)

        # Create and open the popup
        self.input_popup = Popup(title='Enter an Integer', content=input_layout, size_hint=(0.75, 0.5),
                                 auto_dismiss=False)
        self.input_popup.open()

    def on_confirm_input(self, instance):
        # Store the integer value from TextInput
        try:
            self.user_input_number = int(self.input_text.text)
            print(f'User input number: {self.user_input_number}')
        except ValueError:
            print('Invalid input. Please enter an integer.')

        # Close the popup after confirming input
        self.input_popup.dismiss()


if __name__ == '__main__':
    FileChooserApp().run()