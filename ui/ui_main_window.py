import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
ui_logger = logging.getLogger("MyPhotoApp.UI")

from PySide6 import QtCore, QtWidgets, QtGui

from core.core_scanner import scan_directory, FileEntry
from analytics.analytics_db import init_db, get_connection, DB_PATH
from analytics.analytics_writer import insert_entry
from analytics.analytics_queries import (
    get_duplicates_sha256,
    get_live_photos,
    get_corrupted_files,
    get_basic_stats
)




class ScannerWorker(QtCore.QObject):
    """
    Worker object that runs in a separate QThread and scans a directory.
    It writes entries to SQLite and emits progress to the GUI.
    """
    progress = QtCore.Signal(dict)   # emits FileEntry
    finished = QtCore.Signal()
    error = QtCore.Signal(str)

    def __init__(self, directory: str, logger=None):
        super().__init__()
        self._directory = directory
        self._logger = logger
        self._abort = False

    @QtCore.Slot()
    def run(self):
        conn = get_connection()
        try:
            cur = conn.cursor()

            # Limpar completamente as tabelas a cada novo scan
            # Ordem importa por causa das foreign keys
            cur.execute("DELETE FROM hash_meta")
            cur.execute("DELETE FROM image_meta")
            cur.execute("DELETE FROM video_meta")
            cur.execute("DELETE FROM files")
            conn.commit()

            if self._logger:
                self._logger.info("Database cleared before new scan.")

            def cb(entry: FileEntry):
                if self._abort:
                    raise RuntimeError("Scan aborted by user.")

                # Write into SQLite
                insert_entry(conn, entry)

                # Notify GUI
                self.progress.emit(entry)

            scan_directory(self._directory, callback=cb, logger=self._logger)

        except Exception as e:
            if self._logger:
                self._logger.error("Error during scan.", exc_info=True)
            self.error.emit(str(e))
        finally:
            conn.commit()
            conn.close()
            self.finished.emit()


    def abort(self):
        self._abort = True


class FileTableModel(QtCore.QAbstractTableModel):
    HEADERS = ["File path"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data: List[FileEntry] = []

    def rowCount(self, parent=QtCore.QModelIndex()) -> int:
        return len(self._data)

    def columnCount(self, parent=QtCore.QModelIndex()) -> int:
        return 1

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == QtCore.Qt.DisplayRole:
            entry = self._data[index.row()]
            return entry.get("full_path", "")
        return None

    def headerData(self, section, orientation, role=QtCore.Qt.DisplayRole):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return self.HEADERS[section]
        return None

    def add_entry(self, entry: FileEntry):
        row = len(self._data)
        self.beginInsertRows(QtCore.QModelIndex(), row, row)
        self._data.append(entry)
        self.endInsertRows()

    def get_entry(self, row: int) -> FileEntry:
        return self._data[row]

    def clear(self):
        self.beginResetModel()
        self._data.clear()
        self.endResetModel()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, logger=None):
        super().__init__()
        self.logger = logger
        self.setWindowTitle("New Photos - Scanner")
        self.resize(1300, 800)

        # State vars
        self._thread: Optional[QtCore.QThread] = None
        self._worker: Optional[ScannerWorker] = None
        self._scan_running: bool = False
        self._current_directory: Optional[str] = None

        # Scan metrics
        self._files_processed: int = 0
        self._total_files: int = 0
        self._scan_start_time: Optional[datetime] = None

        # Init DB
        init_db()

        self._setup_ui()
        self._create_analysis_menu()
        self._update_analysis_menu_state()

    # ------------------------------------------------------------------
    # UI Setup
    # ------------------------------------------------------------------
    def _setup_ui(self):
        central = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(central)

        # Directory selector
        path_layout = QtWidgets.QHBoxLayout()
        self.path_edit = QtWidgets.QLineEdit()
        self.path_edit.setPlaceholderText("Select a directory to scan...")
        browse_btn = QtWidgets.QPushButton("Browse…")
        browse_btn.clicked.connect(self.on_browse_clicked)
        path_layout.addWidget(self.path_edit)
        path_layout.addWidget(browse_btn)

        # Buttons
        btn_layout = QtWidgets.QHBoxLayout()
        self.scan_btn = QtWidgets.QPushButton("Start scan")
        self.scan_btn.clicked.connect(self.on_scan_clicked)

        self.stop_btn = QtWidgets.QPushButton("Stop")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.on_stop_clicked)

        btn_layout.addWidget(self.scan_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addStretch()

        # Table
        self.table_model = FileTableModel(self)
        self.table_view = QtWidgets.QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.setSortingEnabled(True)
        self.table_view.selectionModel().selectionChanged.connect(self.on_row_selected)

        # Details panel (dynamic dictionary view)
        self.details_panel = self._setup_details_panel()

        splitter = QtWidgets.QSplitter()
        splitter.addWidget(self.table_view)
        splitter.addWidget(self.details_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        # Status bar + progress bar
        self.status_bar = QtWidgets.QStatusBar()
        self.setStatusBar(self.status_bar)

        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(1)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.status_bar.addPermanentWidget(self.progress_bar)

        layout.addLayout(path_layout)
        layout.addLayout(btn_layout)
        layout.addWidget(splitter)
        self.setCentralWidget(central)

    def _setup_details_panel(self) -> QtWidgets.QGroupBox:
        group = QtWidgets.QGroupBox("File details")

        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)

        container = QtWidgets.QWidget()
        self.details_layout = QtWidgets.QFormLayout(container)

        scroll.setWidget(container)

        vbox = QtWidgets.QVBoxLayout(group)
        vbox.addWidget(scroll)
        group.setLayout(vbox)
        return group

    # ------------------------------------------------------------------
    # Analysis menu (Dev-only)
    # ------------------------------------------------------------------
    def _create_analysis_menu(self):
        menubar = self.menuBar()
        self.menu_analysis = menubar.addMenu("Analysis (Dev)")

        self.action_duplicates_sha = QtGui.QAction("SHA256 duplicates", self)
        self.action_duplicates_sha.triggered.connect(self.show_sha_duplicates)

        self.action_live_photos = QtGui.QAction("Live Photos", self)
        self.action_live_photos.triggered.connect(self.show_live_photos)

        self.action_corrupted = QtGui.QAction("Corrupted files", self)
        self.action_corrupted.triggered.connect(self.show_corrupted_files)

        self.action_stats = QtGui.QAction("Basic statistics", self)
        self.action_stats.triggered.connect(self.show_stats)

        self.menu_analysis.addAction(self.action_duplicates_sha)
        self.menu_analysis.addAction(self.action_live_photos)
        self.menu_analysis.addAction(self.action_corrupted)
        self.menu_analysis.addAction(self.action_stats)

    def _update_analysis_menu_state(self):
        db_exists = os.path.exists(DB_PATH)
        enabled = db_exists and not self._scan_running
        self.menu_analysis.setEnabled(enabled)

    # ------------------------------------------------------------------
    # Utility: count files to scan
    # ------------------------------------------------------------------
    def _count_files(self, directory: str) -> int:
        total = 0
        for _, _, files in os.walk(directory):
            total += len(files)
        return total

    def _format_eta(self, seconds: float) -> str:
        """Format ETA in a human-friendly way."""
        if seconds <= 0:
            return "--"
        td = timedelta(seconds=int(seconds))
        if td.total_seconds() < 3600:
            return f"{td.seconds // 60}m {td.seconds % 60}s"
        else:
            return f"{td.seconds // 3600}h {(td.seconds % 3600) // 60}m"

    # ------------------------------------------------------------------
    # Slots: UI actions
    # ------------------------------------------------------------------
    @QtCore.Slot()
    def on_browse_clicked(self):
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Select directory", os.getcwd()
        )
        if directory:
            self.path_edit.setText(directory)

    @QtCore.Slot()
    def on_scan_clicked(self):
        directory = self.path_edit.text().strip()
        if not directory or not os.path.isdir(directory):
            QtWidgets.QMessageBox.warning(self, "Error", "Invalid directory.")
            return

        self._current_directory = directory
        self.table_model.clear()
        self._start_scan(directory)

    @QtCore.Slot()
    def on_stop_clicked(self):
        if self._worker is not None:
            self._worker.abort()
            self.status_bar.showMessage("Stopping scan…")

    @QtCore.Slot()
    def on_row_selected(self):
        indexes = self.table_view.selectionModel().selectedRows()
        if not indexes:
            return
        row = indexes[0].row()
        entry = self.table_model.get_entry(row)
        self.update_details_panel(entry)

    # ------------------------------------------------------------------
    # Worker signals
    # ------------------------------------------------------------------
    @QtCore.Slot(dict)
    def on_worker_progress(self, entry: FileEntry):
        # Update counters
        self._files_processed += 1

        # Update table
        self._add_entry_to_table(entry)

        # Update progress bar
        if self._total_files > 0:
            self.progress_bar.setValue(self._files_processed)

        # Compute speed and ETA
        if self._scan_start_time:
            elapsed = (datetime.now() - self._scan_start_time).total_seconds()
        else:
            elapsed = 0

        if elapsed > 0:
            speed_fpm = (self._files_processed / elapsed) * 60.0
        else:
            speed_fpm = 0.0

        remaining = max(self._total_files - self._files_processed, 0)
        eta_seconds = (remaining / (speed_fpm / 60.0)) if speed_fpm > 0 else 0
        eta_str = self._format_eta(eta_seconds)

        self.status_bar.showMessage(
            f"Scanning… {self._files_processed} / {self._total_files} "
            f"({speed_fpm:.0f} files/min, ETA {eta_str})"
        )

    @QtCore.Slot()
    def on_worker_finished(self):
        self._scan_running = False
        self._update_analysis_menu_state()

        # Final stats
        if self._scan_start_time:
            elapsed = (datetime.now() - self._scan_start_time).total_seconds()
        else:
            elapsed = 0
        speed_fpm = (
            (self._files_processed / elapsed) * 60.0
            if elapsed > 0 else 0.0
        )

        self.scan_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setVisible(False)

        self.status_bar.showMessage(
            f"Scan completed — {self._files_processed} files "
            f"({speed_fpm:.0f} files/min)"
        )

        if self._thread is not None:
            self._thread.quit()
            self._thread.wait()
            self._thread = None
            self._worker = None

    @QtCore.Slot(str)
    def on_worker_error(self, msg: str):
        QtWidgets.QMessageBox.critical(self, "Scan error", msg)
        ui_logger.error(msg)

    # ------------------------------------------------------------------
    # Start scan in background
    # ------------------------------------------------------------------
    def _start_scan(self, directory: str):
        ui_logger.info(f"Starting scan: {directory}")

        # Prepare metrics
        self._files_processed = 0
        self._total_files = self._count_files(directory)
        self._scan_start_time = datetime.now()

        # UI state
        self._scan_running = True
        self._update_analysis_menu_state()

        self.scan_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(max(self._total_files, 1))
        self.progress_bar.setValue(0)

        self.status_bar.showMessage(
            f"Scanning… 0 / {self._total_files} (0 files/min, ETA --)"
        )

        # Thread + worker
        self._thread = QtCore.QThread(self)
        self._worker = ScannerWorker(directory, logger=self.logger)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self.on_worker_progress)
        self._worker.finished.connect(self.on_worker_finished)
        self._worker.error.connect(self.on_worker_error)

        self._thread.start()

    # ------------------------------------------------------------------
    # Details panel
    # ------------------------------------------------------------------
    def update_details_panel(self, entry: Dict[str, Any]):
        # Clear previous widgets
        while self.details_layout.count():
            item = self.details_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        for key, value in entry.items():
            key_label = QtWidgets.QLabel(f"{key}:")
            key_label.setStyleSheet("font-weight: bold;")

            if isinstance(value, (list, dict)):
                value_str = str(value)
            else:
                value_str = "" if value is None else str(value)

            value_label = QtWidgets.QLabel(value_str)
            value_label.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
            value_label.setWordWrap(True)

            self.details_layout.addRow(key_label, value_label)

    def _add_entry_to_table(self, entry: FileEntry):
        self.table_model.add_entry(entry)

    # ------------------------------------------------------------------
    # Analysis dialogs (Dev)
    # ------------------------------------------------------------------
    def _show_analysis_table(self, title: str, rows, headers):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(title)
        dialog.resize(900, 600)
        layout = QtWidgets.QVBoxLayout(dialog)

        table = QtWidgets.QTableWidget(dialog)
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))

        for r, row in enumerate(rows):
            for c, value in enumerate(row):
                item = QtWidgets.QTableWidgetItem(str(value))
                table.setItem(r, c, item)

        table.resizeColumnsToContents()
        layout.addWidget(table)
        dialog.exec()

    def show_sha_duplicates(self):
        conn = get_connection()
        rows = analytics_queries.get_duplicates_sha256(conn)
        conn.close()
        self._show_analysis_table("SHA256 duplicates", rows, ["sha256", "count"])

    def show_live_photos(self):
        conn = get_connection()
        rows = analytics_queries.get_live_photos(conn)
        conn.close()
        self._show_analysis_table("Live Photos", rows, ["jpg", "mov"])

    def show_corrupted_files(self):
        conn = get_connection()
        rows = analytics_queries.get_corrupted_files(conn)
        conn.close()
        self._show_analysis_table("Corrupted files", rows, ["path", "error"])

    def show_stats(self):
        conn = get_connection()
        stats = analytics_queries.get_basic_stats(conn)
        conn.close()

        msg = QtWidgets.QMessageBox(self)
        msg.setWindowTitle("Statistics")
        msg.setText(
            f"Total files: {stats['total']}\n"
            f"Images: {stats['images']}\n"
            f"Videos: {stats['videos']}\n"
            f"Corrupted: {stats['corrupted']}"
        )
        msg.exec()