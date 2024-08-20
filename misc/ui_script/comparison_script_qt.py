import configparser
import csv
import sys
import subprocess
import os
import xml.etree.ElementTree as ET
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QCheckBox, QDateEdit, QPushButton, QMessageBox
from PyQt5.QtCore import Qt, QDateTime


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.initUI()
        self.product_lines = {}
        self.assemblies = {}
        self.load_data('product_assembly_conf.csv')

    def initUI(self):
        self.setWindowTitle('Product and Assembly Selection')
        #Layouts
        main_layout = QVBoxLayout()
        product_line_layout = QHBoxLayout()
        assemblies_layout = QHBoxLayout()
        date_layout = QHBoxLayout()
        button_layout = QHBoxLayout()

        #Product line selection
        self.product_line_label = QLabel("Select Product Line:")
        self.product_line_list =  QListWidget()
        self.product_line_list.setSelectionMode(QListWidget.MultiSelection)
        self.product_line_list.itemSelectionChanged.connect(self.update_assemblies)

        #Assembly selection
        self.assembly_label = QLabel("Select Assemblies: ")
        self.assembly_list = QListWidget()
        self.assembly_list.setSelectionMode(QListWidget.MultiSelection)

        #Date selection
        self.from_date_label = QLabel("From Date: ")
        self.from_date_edit = QDateEdit()
        self.from_date_edit.setCalendarPopup(True)
        self.from_date_edit.setDisplayFormat('yyyy-MM-ddTHH:mm:ss')
        self.from_date_edit.setDateTime(QDateTime.currentDateTime())

        self.to_date_label = QLabel("To Date: ")
        self.to_date_edit = QDateEdit()
        self.to_date_edit.setCalendarPopup(True)
        self.to_date_edit.setDisplayFormat('yyyy-MM-ddTHH:mm:ss')
        self.to_date_edit.setDateTime(QDateTime.currentDateTime())

        #Run button
        self.run_button = QPushButton('Run')
        self.run_button.clicked.connect(self.run_program)

        #Adding widgets to layout
        product_line_layout.addWidget(self.product_line_label)
        product_line_layout.addWidget(self.product_line_list)

        assemblies_layout.addWidget(self.assembly_label)
        assemblies_layout.addWidget(self.assembly_list)

        date_layout.addWidget(self.from_date_label)
        date_layout.addWidget(self.from_date_edit)
        date_layout.addWidget(self.to_date_label)
        date_layout.addWidget(self.to_date_edit)

        button_layout.addWidget(self.run_button)

        #adding layouts to main layout
        main_layout.addLayout(product_line_layout)
        main_layout.addLayout(assemblies_layout)
        main_layout.addLayout(date_layout)
        main_layout.addLayout(button_layout)

        self.setLayout(main_layout)

    def load_data(self, filename):
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                product_line, assembly= row
                if product_line not in self.product_lines:
                    self.product_lines[product_line] = []
                self.product_lines[product_line].append(assembly)

            for product_line in self.product_lines:
                item = QListWidgetItem(product_line)
                self.product_line_list.addItem(item)

        # Add "All Products" option
        item = QListWidgetItem("All Product Lines")
        self.product_line_list.addItem(item)
    def update_assemblies(self):
        selected_product_lines = [item.text() for item in self.product_line_list.selectedItems()]
        self.assembly_list.clear()

        if "All Product Lines" in selected_product_lines:
            selected_product_lines = list(self.product_lines.keys())

            # all_assemblies = set()
            all_assemblies = []
            for product_line in selected_product_lines:
                all_assemblies.extend(self.product_lines[product_line])

            for assembly in all_assemblies:
                item = QListWidgetItem(assembly)
                self.assembly_list.addItem(item)
            # Select all items in assembly_list
            for i in range(self.assembly_list.count()):
                self.assembly_list.item(i).setSelected(True)
        else:
            for product_line in selected_product_lines:
                all_assemblies = self.product_lines[product_line]
                for assembly in all_assemblies:
                    item = QListWidgetItem(assembly)
                    self.assembly_list.addItem(item)

    def run_program(self):
        selected_product_lines = [item.text() for item in self.product_line_list.selectedItems()]
        selected_assemblies = [item.text() for item in self.assembly_list.selectedItems()]
        from_date = self.from_date_edit.dateTime().toString("yyyy-MM-ddTHH:mm:ss")
        to_date = self.to_date_edit.dateTime().toString("yyyy-MM-ddTHH:mm:ss")

        if not selected_product_lines:
            QMessageBox.warning(self, "Input Error", "Please select at least one product line")
            return

        if not selected_assemblies:
            QMessageBox.warning(self, "Input Error", "Please select at least one assembly")
            return

        # print(selected_product_lines)
        # print(selected_assemblies)

        try:
            #Update the configuraiton file
            APP_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),r'..\..\conf\app.conf'))
            parser = configparser.ConfigParser()
            parser.read(APP_CONF_PATH)
            parser['PRODUCT_ASSEMBLY_DATA']['product_line']=', '.join(selected_product_lines)
            parser['PRODUCT_ASSEMBLY_DATA']['assembly']=', '.join(selected_assemblies)
            parser['DATE_RANGE']['from_date']=from_date
            parser['DATE_RANGE']['to_date']=to_date

            with open(APP_CONF_PATH, 'w') as configFile:
                parser.write(configFile)

            #Call main.py using subprocess
            main_script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), r"..\..\main.py"))
            subprocess.Popen(['python', main_script_path])
        except Exception as e:
            print("error in updating conf file", e)


        # #Update the selected values labels
        # self.product_line_label.setText(f"Selected Product Line: {', '.join(selected_product_lines)}")
        # self.assembly_label.setText(f"Selected Assembly: {', '.join(selected_assemblies)}")
        # self.from_date_label.setText(f"From Date: {from_date}")
        # self.from_date_label.setText(f"To Date: {to_date}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())

