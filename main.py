import sys
from PySide6 import QtWidgets

from logging_config import setup_logging
from ui.ui_main_window import MainWindow



def main():
    logger = setup_logging()
    logger.info("Application started.")

    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow(logger=logger)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
