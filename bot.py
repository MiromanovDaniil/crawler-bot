import os
import logging
import pandas as pd
import asyncio
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters
)
from database import DatabaseHandler 
from dotenv import load_dotenv
from parser import PriceScraper 

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

db_handler = DatabaseHandler()

class PriceBot:
    def __init__(self, token: str):
        """Initialize the bot with a token and set up directories and handlers."""
        self.application = Application.builder().token(token).build()
        self.uploads_dir = "uploads"
        self._create_uploads_dir()
        self.setup_handlers()

    def _create_uploads_dir(self):
        """Create the uploads directory if it doesn't exist."""
        os.makedirs(self.uploads_dir, exist_ok=True)

    def setup_handlers(self):
        """Initialize all bot handlers."""
        handlers = [
            CommandHandler('start', self.start),
            MessageHandler(filters.TEXT & filters.Regex(r'^Загрузить файл$'), 
                           self.handle_file_prompt),
            MessageHandler(filters.Document.FileExtension("xlsx"), 
                           self.handle_file),
        ]
        for handler in handlers:
            self.application.add_handler(handler)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start command and display the initial keyboard."""
        keyboard = [["Загрузить файл"]]
        await update.message.reply_text(
            "Отправьте Excel файл с данными:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )

    async def handle_file_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Prompt the user to upload an Excel file."""
        await update.message.reply_text(
            "Пожалуйста, отправьте Excel файл через значок скрепки (📎) выбрав .xlsx файл."
        )

    async def handle_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process the uploaded Excel file."""
        user = update.message.from_user
        try:
            file = await update.message.document.get_file()
            file_path = os.path.join(self.uploads_dir, f"{user.id}.xlsx")
            await file.download_to_drive(file_path)
            await self.process_excel_file(update, file_path, user)
        except Exception as e:
            logger.error(f"File handling error for user {user.id}: {e}")
            await update.message.reply_text("❌ Ошибка обработки файла!")

    async def process_excel_file(self, update: Update, file_path: str, user):
        """Process the downloaded Excel file and scrape prices."""
        try:
            await update.message.reply_text("⏳ Обработка файла...")

            df = pd.read_excel(file_path)
            required_columns = {'title', 'url', 'xpath'}
            
            if not required_columns.issubset(df.columns):
                raise ValueError("Invalid file structure")
            
            loop = asyncio.get_running_loop()
            with PriceScraper() as scraper:
                prices = await loop.run_in_executor(None, self.process_dataframe, df, scraper)

            df['parsed_price'] = prices
            valid_prices = [p for p in prices if p is not None]
            
            await update.message.reply_text("✅ Обработка завершена!")

            await self.send_results(update, len(df), valid_prices)
            
            db_handler.save_to_db(user.id, df) 
            
        except ValueError as ve:
            logger.warning(f"Invalid file from user {user.id}: {ve}")
            await update.message.reply_text("❌ Неверный формат файла! Требуемые колонки: title, url, xpath")
        except Exception as e:
            logger.error(f"Processing error for user {user.id}: {e}")
            await update.message.reply_text("❌ Ошибка при обработке данных в файле!")

    def process_dataframe(self, df: pd.DataFrame, scraper: PriceScraper) -> list:
        """Process the DataFrame synchronously and collect prices using PriceScraper."""
        prices = []
        for _, row in df.iterrows():
            try:
                price = scraper.scrape_price(row['url'], row['xpath'])
                prices.append(price)
            except Exception as e:
                logger.warning(f"Failed to scrape {row['url']}: {e}")
                prices.append(None)
        return prices

    async def send_results(self, update: Update, total_attempts: int, valid_prices: list):
        """Send the processing results to the user with detailed feedback."""
        if not valid_prices:
            await update.message.reply_text("⚠️ Не удалось получить цены с указанных сайтов!")
            return

        avg_price = sum(valid_prices) / len(valid_prices)
        response = (
            f"Результаты обработки:\n"
            f"• Всего сайтов: {total_attempts}\n"
            f"• Успешно получено цен: {len(valid_prices)}\n"
            f"• Средняя цена: {avg_price:.2f} руб."
        )
        await update.message.reply_text(response)

    def run(self):
        """Start the bot."""
        logger.info("Starting price monitoring bot...")
        self.application.run_polling()

if __name__ == '__main__':
    bot = PriceBot(os.getenv("BOT_TOKEN"))
    bot.run()