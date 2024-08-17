import faust

# Define the Faust application
app = faust.App('my_faust_app', broker='kafka://192.168.0.107:9092')


# Define a model for the data
class MyRecord(faust.Record, serializer='json'):
    key: str
    value: int


# Define a table to store the latest values
my_table = app.Table('HIGH_ENGAGEMENT_TBALE', default=int)


# Create a Faust web view to query the table with a range filter
@app.page('/query_range/{min_value}/')
async def get_values_in_range(self, request, min_value: int):
    results = {}
    for key, value in my_table.items():
        if value > min_value:
            results[key] = value
    return self.json(results)


if __name__ == '__main__':
    app.main()
