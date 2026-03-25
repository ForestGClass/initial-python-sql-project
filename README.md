# Python SQL Analysis Project

This project connects Python to SQL Server and performs simple analysis of betting and payment data.

## Features
- Connects to SQL Server using pyodbc
- Loads aggregated data with SQL
- Calculates total bets, total payments, and profit per user
- Exports results to CSV
- Generates a bar chart for top users

## Technologies
- Python
- pandas
- pyodbc
- SQL Server
- matplotlib

## Setup

Create a `.env` file in the root directory based on `.env.example`.

Example:

DB_HOST=localhost  
DB_PORT=1433  
DB_NAME=betting_project  
DB_USER=sa  
DB_PASSWORD=your_password_here  

Update these values according to your local database configuration.

## Run

```bash
python main.py
