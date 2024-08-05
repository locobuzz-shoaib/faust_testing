from typing import Optional

import faust

app = faust.App('alert_app', broker='kafka://172.18.244.10:9092')


# Define the schema of the incoming data
class Post(faust.Record, serializer='json'):
    mentionid: str
    uniqueid: str
    createdate: str
    numlikes: int
    numcomments: int
    numshare: int
    socialid: Optional[str]
    postsocialid: Optional[str]
    description: str


# Define the input topic
finaldata_topic = app.topic('finaldata', value_type=Post)

# Define a table to store the aggregated data
posts_table = app.Table('posts_table', default=dict)


@app.agent(finaldata_topic)
async def process_post(posts):
    async for post in posts:
        if not post.postsocialid:
            # This is a parent post
            parent_post = posts_table[post.socialid]
            parent_post['numlikes'] = post.numlikes
            parent_post['numcomments'] = post.numcomments
        else:
            # This is a child comment
            parent_post = posts_table[post.postsocialid]
            parent_post['numcomments'] = parent_post.get('numcomments', 0) + 1

        # Check if the alert condition is met
        if parent_post['numcomments'] > 10000 and parent_post['numlikes'] >= 50:
            send_alert(post.socialid, parent_post)


def send_alert(socialid, post_data):
    # Implement the logic to send an email alert
    print(f"Alert: Post {socialid} has {post_data['numcomments']} comments and {post_data['numlikes']} likes.")


if __name__ == '__main__':
    app.main()
