import faust


# Define the transaction model
class Transaction(faust.Record):
    customer_id: int
    amount: float


# Create a Faust app
app = faust.App('transaction_filter', broker='kafka://localhost:9092')

# Define a Kafka topic to read the transactions
transactions_topic = app.topic('transactions', value_type=Transaction)

# Define a Kafka topic to write the filtered transactions
filtered_transactions_topic = app.topic('filtered_transactions', value_type=Transaction)


# Define an agent to filter transactions
@app.agent(transactions_topic)
async def filter_transactions(transactions):
    async for transaction in transactions:
        if transaction.amount >= 100:
            await filtered_transactions_topic.send(value=transaction)
            print(f"Filtered Transaction: {transaction}")


# Start the Faust app
if __name__ == '__main__':
    app.main()
