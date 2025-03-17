import sqlite3
import logging
import pandas as pd
from typing import Optional, List, Dict

class DatabaseHandler:
    def __init__(self, db_name: str = 'sites.db'):
        """
        Инициализация класса для работы с базой данных.
        :param db_name: Имя файла базы данных (по умолчанию 'sites.db').
        """
        self.db_name = db_name
        self.init_db()

    def create_connection(self) -> sqlite3.Connection:
        """
        Создает и возвращает соединение с базой данных.
        """
        return sqlite3.connect(self.db_name)

    def init_db(self) -> None:
        """
        Инициализирует базу данных, создает таблицу, если она не существует.
        """
        conn = self.create_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                xpath TEXT NOT NULL,
                parsed_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, url)
            )
        ''')
        conn.commit()
        conn.close()

    def save_to_db(self, user_id: int, dataframe: pd.DataFrame) -> bool:
        """
        Сохраняет данные из DataFrame в таблицу 'sites'.
        :param user_id: ID пользователя.
        :param dataframe: DataFrame с данными для сохранения.
        :return: True, если данные успешно сохранены, иначе False.
        """
        conn = self.create_connection()
        try:
            dataframe['user_id'] = user_id
            dataframe = dataframe[['user_id', 'title', 'url', 'xpath', 'parsed_price']]
            dataframe.to_sql('sites', conn, if_exists='append', index=False)
            return True
        except Exception as e:
            logging.error(f"Database error: {e}")
            return False
        finally:
            conn.close()

    def view_data(self) -> Optional[pd.DataFrame]:
        """
        Возвращает все данные из таблицы 'sites' в виде DataFrame.
        :return: DataFrame с данными или None в случае ошибки.
        """
        conn = self.create_connection()
        try:
            df = pd.read_sql_query("SELECT * FROM sites", conn)
            return df
        except Exception as e:
            logging.error(f"Ошибка при чтении данных: {e}")
            return None
        finally:
            conn.close()

    def delete_data(self, user_id: Optional[int] = None) -> bool:
        """
        Удаляет данные из таблицы 'sites'.
        :param user_id: Если указан, удаляет данные только для этого пользователя.
        :return: True, если удаление прошло успешно, иначе False.
        """
        conn = self.create_connection()
        try:
            if user_id:
                conn.execute("DELETE FROM sites WHERE user_id = ?", (user_id,))
            else:
                conn.execute("DELETE FROM sites")
            conn.commit()
            return True
        except Exception as e:
            logging.error(f"Ошибка при удалении данных: {e}")
            return False
        finally:
            conn.close()

if __name__ == "__main__":
    db_handler = DatabaseHandler()
    print("Данные из таблицы 'sites':")
    data_df = db_handler.view_data()
    if data_df is not None:
        print(data_df)
