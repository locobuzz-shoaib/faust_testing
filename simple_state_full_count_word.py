import faust


class Word(faust.Record):
    word: str


app = faust.App('word_count_app', broker='kafka://localhost:9092')

words_topic = app.topic('words_topic', value_type=Word)
counts_table = app.Table('word_counts', default=int)


@app.agent(words_topic)
async def count_words(words):
    async for word in words.group_by(Word.word):
        counts_table[word.word] += 1
        print(f'Word: {word.word}, Count: {counts_table[word.word]}')


if __name__ == '__main__':
    app.main()
