import sys
import os
import shutil
from pathlib import Path
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QTextEdit,
    QProgressBar, QListWidget, QHBoxLayout, QFileDialog, QMessageBox
)
from PySide6.QtCore import Qt, QThread, Signal, QPoint
from PySide6.QtGui import QFont, QCursor, QPixmap, QIcon
from refine import refine_sql  # ensure refine.py has refine_sql(input_file, log_func)

# --- Results directory ---
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)  # create on first run

# --- Temporary folders to clean ---
TEMP_FOLDERS = [Path("chunks"), Path("sqls")]

def clean_temp_folders():
    for folder in TEMP_FOLDERS:
        if folder.exists() and folder.is_dir():
            shutil.rmtree(folder)
            print(f"🧹 Deleted temporary folder: {folder}")

# Clean temp folders at startup
clean_temp_folders()

# --- Worker thread ---
class RefineWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)

    def __init__(self, input_file: str):
        super().__init__()
        self.input_file = input_file

    def run(self):
        try:
            self.log("Starting refinement...")

            def log_func(msg, progress=None):
                self.log(msg)
                if progress is not None:
                    self.progress_signal.emit(progress)

            output_file = refine_sql(self.input_file, log_func)
            if output_file:
                # Save directly to RESULTS_DIR
                final_path = RESULTS_DIR / Path(output_file).name
                shutil.move(output_file, final_path)
                self.finished_signal.emit(str(final_path))
            else:
                self.finished_signal.emit("")
        except Exception as e:
            self.log(f"Error: {e}")
            self.finished_signal.emit("")

    def log(self, message):
        self.log_signal.emit(message)

# --- Main GUI ---
class RefineApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Refine SQL")
        self.setGeometry(250, 100, 900, 570)
        self.setStyleSheet("background-color: #2c2c2c; color: #eee;")
        self.setWindowIcon(QIcon("logo.png"))

        main_layout = QVBoxLayout()

        # --- Small logo above title ---
        logo = QLabel()
        pixmap = QPixmap("logo.png")
        if not pixmap.isNull():
            pixmap = pixmap.scaled(40, 40, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo.setPixmap(pixmap)
            logo.setAlignment(Qt.AlignCenter)
            main_layout.addWidget(logo)

        # --- Title & Subtitle ---
        title = QLabel("Refine SQL")
        title.setFont(QFont("Arial", 22, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("color: #FF9800;")
        main_layout.addWidget(title)

        subtitle = QLabel("Refine your large MySQL dumps into clean and error-free SQL files")
        subtitle.setFont(QFont("Arial", 11))
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet("color: #EEEEE4;")
        main_layout.addWidget(subtitle)

        # --- Upload button ---
        self.upload_btn = QPushButton("Upload SQL File")
        self.upload_btn.setCursor(QCursor(Qt.PointingHandCursor))
        self.upload_btn.clicked.connect(self.upload_file)
        self.upload_btn.setStyleSheet("""
            QPushButton {
                background-color: #2196F3; color: white; border-radius: 6px; padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
        """)
        main_layout.addWidget(self.upload_btn)

        # --- Progress bar ---
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setAlignment(Qt.AlignCenter)
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setStyleSheet(self.get_progress_style())
        main_layout.addWidget(self.progress_bar)

        # --- Log area ---
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setStyleSheet("background-color: #1e1e1e; color: #fff;")
        main_layout.addWidget(self.log_area)

        # --- Result list ---
        self.result_list = QListWidget()
        self.result_list.setStyleSheet("""
            QListWidget { 
                background-color: #2c2c2c;  
                color: #fff; 
                border: 1px solid #555;
                border-radius: 5px;
            }
            QListWidget::item { 
                background-color: #4a4a4a;  
            }
            QListWidget::item:hover { 
                background-color: #2196F3;
            }
            QListWidget::item:selected {
                background-color: #2196F3;
                color: #fff;
            }
        """)
        self.result_list.setCursor(QCursor(Qt.PointingHandCursor))
        self.load_results()
        self.result_list.mousePressEvent = self.list_mouse_press
        main_layout.addWidget(self.result_list)

        # --- Action buttons ---
        btn_layout = QHBoxLayout()
        self.delete_btn = QPushButton("Delete Selected Result")
        self.delete_btn.setCursor(QCursor(Qt.PointingHandCursor))
        self.delete_btn.clicked.connect(self.delete_selected)
        self.delete_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336; color: white; border-radius: 6px; padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #d32f2f;
            }
        """)
        btn_layout.addWidget(self.delete_btn)

        self.copy_btn = QPushButton("Save Selected Refined File")
        self.copy_btn.setCursor(QCursor(Qt.PointingHandCursor))
        self.copy_btn.clicked.connect(self.copy_selected)
        self.copy_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50; color: white; border-radius: 6px; padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """)
        btn_layout.addWidget(self.copy_btn)

        main_layout.addLayout(btn_layout)

        # --- Footer ---
        footer = QLabel(
            'Developed By <a href="https://pigeonic.com" style="color:#00BFFF; text-decoration:none;">Pigeonic</a> | '
            'Version 1.0.0 | © 2025 Pigeonic. All rights reserved.'
        )
        footer.setTextFormat(Qt.RichText)
        footer.setTextInteractionFlags(Qt.TextBrowserInteraction)
        footer.setOpenExternalLinks(True)
        footer.setAlignment(Qt.AlignCenter)
        footer.setFont(QFont("Arial", 10))
        footer.setStyleSheet("""
            QLabel { color: #AAAAAA; }
            a { color: #00BFFF; text-decoration: none; }
            a:hover { color: #1E90FF; }
        """)
        main_layout.addWidget(footer)

        self.setLayout(main_layout)
        self.worker = None

    # --- Progress bar style ---
    def get_progress_style(self):
        return """
            QProgressBar {
                border: 1px solid #AAA;
                border-radius: 5px;
                text-align: center;
                background-color: #444;
                color: #fff;
            }
            QProgressBar::chunk {
                background-color: #4CAF50;
                width: 1px;
            }
        """

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    # --- Deselect on click ---
    def list_mouse_press(self, event):
        pos = event.position()
        item = self.result_list.itemAt(QPoint(int(pos.x()), int(pos.y())))
        if item and item.isSelected():
            item.setSelected(False)
        else:
            QListWidget.mousePressEvent(self.result_list, event)

    # --- GUI methods ---
    def upload_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select SQL File", "", "SQL Files (*.sql)")
        if not file_path:
            return
        self.log_area.clear()
        self.progress_bar.setValue(0)

        self.worker = RefineWorker(file_path)
        self.worker.log_signal.connect(self.append_log)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.finished_signal.connect(self.refine_done)
        self.worker.start()

    def append_log(self, message, progress=None):
        self.log_area.append(message)
        if progress is not None:
            self.update_progress(progress)

    def refine_done(self, final_file):
        if final_file:
            self.append_log(f"✅ Refinement complete: {final_file}")
            self.progress_bar.setValue(100)
            self.load_results()
        else:
            self.append_log("❌ Refinement failed!")

    def load_results(self):
        self.result_list.clear()
        for file in RESULTS_DIR.glob("*.sql"):
            self.result_list.addItem(str(file))

    def delete_selected(self):
        selected = self.result_list.currentItem()
        if selected and os.path.exists(selected.text()):
            confirm = QMessageBox.question(
                self,
                "Confirm Delete",
                f"Are you sure you want to delete this file?\n\n{selected.text()}",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if confirm == QMessageBox.Yes:
                os.remove(selected.text())
                self.load_results()

    def copy_selected(self):
        selected = self.result_list.currentItem()
        if selected and os.path.exists(selected.text()):
            folder = QFileDialog.getExistingDirectory(self, "Select Folder to Copy File")
            if folder:
                dest = Path(folder) / Path(selected.text()).name
                shutil.copy(selected.text(), dest)
                self.append_log(f"📄 File copied to: {dest}")

    # --- Override closeEvent to clean temp folders ---
    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Refinement in progress",
                "Refinement is still running. Closing will delete temporary files. Are you sure?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.worker.terminate()
                self.worker.wait()
                clean_temp_folders()
                event.accept()
            else:
                event.ignore()
        else:
            clean_temp_folders()
            event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RefineApp()
    window.show()
    sys.exit(app.exec())
