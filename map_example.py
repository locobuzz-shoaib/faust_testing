import faust


class Transaction(faust.Record):
    customer_id: int
    amount: float


class ProcessedTransaction(faust.Record):
    customer_id: int
    amount: float
    processed: bool


app = faust.App('transaction_app', broker='kafka://localhost:9092')

transactions_topic = app.topic('transactions', value_type=Transaction)
processed_transactions_topic = app.topic('processed_transactions', value_type=ProcessedTransaction)


@app.agent(transactions_topic)
async def process(transactions):
    async for transaction in transactions:
        processed_transaction = ProcessedTransaction(
            customer_id=transaction.customer_id,
            amount=transaction.amount,
            processed=True
        )
        await processed_transactions_topic.send(value=processed_transaction)
        print(f"Processed Transaction: {processed_transaction}")


if __name__ == '__main__':
    app.main()
