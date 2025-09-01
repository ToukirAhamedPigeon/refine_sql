import sys
import os
from pathlib import Path
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QTextEdit,
    QProgressBar, QListWidget, QHBoxLayout, QFileDialog
)
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QFont

# Import your refine logic
from refine import refine_sql  # ensure refine.py has refine_sql(input_file, log_func)

# --- Results directory ---
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

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
            self.finished_signal.emit(output_file)
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
        self.setGeometry(250, 100, 900, 700)

        main_layout = QVBoxLayout()

        # --- Title & Subtitle ---
        title = QLabel("Refine SQL")
        title.setFont(QFont("Arial", 24, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title)

        subtitle = QLabel("Refine your older, large MySQL dumped files with Errors")
        subtitle.setFont(QFont("Arial", 12))
        subtitle.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(subtitle)

        # --- Buttons, Progress, Log ---
        self.upload_btn = QPushButton("Upload SQL File")
        self.upload_btn.clicked.connect(self.upload_file)
        main_layout.addWidget(self.upload_btn)

        self.progress_bar = QProgressBar()
        main_layout.addWidget(self.progress_bar)

        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        main_layout.addWidget(self.log_area)

        # --- Result list ---
        self.result_list = QListWidget()
        self.load_results()
        main_layout.addWidget(self.result_list)

        btn_layout = QHBoxLayout()
        self.delete_btn = QPushButton("Delete Selected Result")
        self.delete_btn.clicked.connect(self.delete_selected)
        btn_layout.addWidget(self.delete_btn)

        self.open_btn = QPushButton("Open Refined File")
        self.open_btn.clicked.connect(self.open_selected)
        btn_layout.addWidget(self.open_btn)

        main_layout.addLayout(btn_layout)

        # --- Footer ---
        footer = QLabel(
            'Developed By <a href="https://pigeonic.com">Pigeonic</a> | '
            'Version 1.0.0 | © 2025 Pigeonic. All rights reserved.'
        )
        footer.setOpenExternalLinks(True)
        footer.setAlignment(Qt.AlignCenter)
        footer.setFont(QFont("Arial", 10))
        main_layout.addWidget(footer)

        self.setLayout(main_layout)
        self.worker = None

    # --- Methods ---
    def upload_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select SQL File", "", "SQL Files (*.sql)")
        if not file_path:
            return
        self.log_area.clear()
        self.progress_bar.setValue(0)

        self.worker = RefineWorker(file_path)
        self.worker.log_signal.connect(self.append_log)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self.refine_done)
        self.worker.start()

    def append_log(self, message):
        self.log_area.append(message)

    def refine_done(self, output_file):
        if output_file:
            self.append_log(f"✅ Refinement complete: {output_file}")
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
            os.remove(selected.text())
            self.load_results()

    def open_selected(self):
        selected = self.result_list.currentItem()
        if selected and os.path.exists(selected.text()):
            os.startfile(selected.text())


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RefineApp()
    window.show()
    sys.exit(app.exec())
