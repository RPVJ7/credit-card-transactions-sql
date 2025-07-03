# Credit Card Transactions SQL Analysis Project

## Project Overview
This SQL project explores a real-world dataset of credit card transactions across Indian cities. It answers complex business questions using SQL techniques such as CTEs, window functions, aggregations, and filtering. The goal is to derive insights into customer behavior, spend patterns, and card performance.

The project is part of a personal data portfolio to showcase business analytics thinking and advanced SQL skills.

---

## Dataset Description

The dataset consists of one main table: `credit_card_transcations`, with the following columns:

- `transaction_id`: Unique identifier of the transaction  
- `city`: City where transaction occurred  
- `transaction_date`: Date of the transaction  
- `card_type`: Type of card used (Gold, Platinum, Silver, Signature)  
- `exp_type`: Type of expense (Bills, Fuel, etc.)  
- `gender`: Gender of cardholder (M/F)  
- `amount`: Amount spent in the transaction  

---

## Business Questions Answered

1. Top 5 cities by credit card spends and their percentage of total spend  
2. Highest spending month for each card type  
3. First transaction to cross 1,000,000 cumulative spend per card type  
4. City with lowest percentage spend using Gold cards  
5. For each city, find highest and lowest expense types  
6. Female spend contribution by each expense category  
7. Card and expense type combo with highest MoM growth in Jan 2014  
8. On weekends, which city had highest spend-to-transaction ratio?  
9. Which city reached its 500th transaction the fastest?  

---

## Skills Demonstrated

- SQL CTEs and window functions (`ROW_NUMBER`, `SUM OVER`)  
- Month-over-month growth analysis  
- Gender-based segmentation and contribution  
- Ranking and filtering using subqueries  
- Time series and cumulative analysis  
- Data quality filters (non-zero, valid joins)

---

## Business Insights

- Identify high-spend cities and card types for promotions  
- Analyze expense preferences by geography  
- Understand female purchasing behavior across categories  
- Optimize campaigns for card-expense type combinations  
- Detect spend surges or growth opportunities

---

## Project Files

- `credit_card_transactions.sql` – SQL queries solving 9 complex problems  
- `credit_card_transcations.xlsx` – Raw dataset with transaction details  

---

## How to Use

1. Load the dataset into your SQL engine (MySQL, PostgreSQL, etc.)  
2. Run the queries in `credit_card_transactions.sql`  
3. Review outputs and tweak for additional KPIs  

---

> This project is a part of my data portfolio to demonstrate applied SQL analytics on finance-related datasets.

