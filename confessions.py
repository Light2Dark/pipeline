from facebook_scraper import get_posts
import pandas as pd
from dotenv import load_dotenv
import os

# as get_posts extracts all data, I will store the raw data in a csv file

"""
{'post_id': '621748629305016', 'text': '#TCP329719\nLokasi? Rahsia.', 'post_text': '#TCP329719\nLokasi? Rahsia.', 'shared_text': '', 'original_text': None, 'time': datetime.datetime(2022, 9, 9, 12, 22, 6), 'timestamp': 1662697326, 'image': 'https://scontent.fkul8-1.fna.fbcdn.net/v/t39.30808-6/305931722_621748619305017_706374956822678174_n.jpg?stp=cp0_dst-jpg_e15_fr_q65&_nc_cat=107&ccb=1-7&_nc_sid=2d5d41&_nc_ohc=wE9hVFeRNlQAX874Z0f&_nc_ht=scontent.fkul8-1.fna&oh=00_AT9c1dGkUc82LU9lIK5fzzzxkBf2hHLZkjnQ3CLdNxoF2g&oe=6320E2CC', 'image_lowquality': 'https://scontent.fkul8-1.fna.fbcdn.net/v/t39.30808-6/305931722_621748619305017_706374956822678174_n.jpg?stp=cp0_dst-jpg_e15_p320x320_q65&_nc_cat=107&ccb=1-7&_nc_sid=2d5d41&_nc_ohc=wE9hVFeRNlQAX874Z0f&_nc_ht=scontent.fkul8-1.fna&oh=00_AT9VILJbMcvG5ZtKBW9--0h0qrScr894-aUsJLOLFnXTWA&oe=6320E2CC', 'images': ['https://scontent.fkul8-1.fna.fbcdn.net/v/t39.30808-6/305931722_621748619305017_706374956822678174_n.jpg?stp=cp0_dst-jpg_e15_fr_q65&_nc_cat=107&ccb=1-7&_nc_sid=2d5d41&_nc_ohc=wE9hVFeRNlQAX874Z0f&_nc_ht=scontent.fkul8-1.fna&oh=00_AT9c1dGkUc82LU9lIK5fzzzxkBf2hHLZkjnQ3CLdNxoF2g&oe=6320E2CC'], 'images_description': ['Mungkin imej makanan'], 'images_lowquality': ['https://scontent.fkul8-1.fna.fbcdn.net/v/t39.30808-6/305931722_621748619305017_706374956822678174_n.jpg?stp=cp0_dst-jpg_e15_p320x320_q65&_nc_cat=107&ccb=1-7&_nc_sid=2d5d41&_nc_ohc=wE9hVFeRNlQAX874Z0f&_nc_ht=scontent.fkul8-1.fna&oh=00_AT9VILJbMcvG5ZtKBW9--0h0qrScr894-aUsJLOLFnXTWA&oe=6320E2CC'], 'images_lowquality_description': ['Mungkin imej makanan'], 'video': None, 'video_duration_seconds': None, 'video_height': None, 'video_id': None, 'video_quality': None, 'video_size_MB': None, 'video_thumbnail': None, 'video_watches': None, 'video_width': None, 'likes': 7, 'comments': 5, 'shares': 0, 'post_url': 'https://facebook.com/taylorsconfessions1/posts/621748629305016', 'link': None, 'links': [{'link': '/hashtag/tcp329719?__tn__=%2As-R', 'text': '#TCP329719'}, {'link': '/story.php?story_fbid=621748629305016&substory_index=0&id=106408717505679&m_entstream_source=timeline&__tn__=%2As%2As-R', 'text': ''}, {'link': '/taylorsconfessions1/photos/a.107341577412393/621748629305016/?type=3&source=48&__tn__=EH-R', 'text': ''}], 'user_id': '106408717505679', 'username': 'Taylors Confessions', 'user_url': 'https://facebook.com/taylorsconfessions1/?__tn__=C-R', 'is_live': False, 'factcheck': None, 'shared_post_id': None, 'shared_time': None, 'shared_user_id': None, 'shared_username': None, 'shared_post_url': None, 'available': True, 'comments_full': None, 'reactors': None, 'w3_fb_url': None, 'reactions': None, 'reaction_count': 7, 'with': None, 'page_id': '106408717505679', 'sharers': None, 'image_id': '621748629305016', 'image_ids': ['621748629305016'], 'was_live': False}
"""

def scrape(pages: int, posts_per_page: int = 4):
    df = pd.DataFrame()
    postNum = 0
    load_dotenv()
    
    # we remove all properties that are array-based to avoid uneven length of rows
    for post in get_posts('taylorsconfessions1', pages=pages, credentials=(os.environ["FB_EMAIL"], os.environ["FB_PASSWORD"]), options={"posts_per_page": posts_per_page}):
        post_df = pd.DataFrame({
            "post_id": post["post_id"],
            "text": post["text"],
            "post_text": post["post_text"],
            "shared_text": post["shared_text"],
            "original_text": post["original_text"],
            "time": post["time"],
            "timestamp": post["timestamp"],
            "image": post["image"],
            "image_lowquality": post["image_lowquality"],
            "video": post["video"],
            "video_duration_seconds": post["video_duration_seconds"],
            "video_height": post["video_height"],
            "video_id": post["video_id"],
            "video_quality": post["video_quality"],
            "video_size_MB": post["video_size_MB"],
            "video_thumbnail": post["video_thumbnail"],
            "video_watches": post["video_watches"],
            "likes": post["likes"],
            "comments": post["comments"],
            "shares": post["shares"],
            "post_url": post["post_url"],
            "link": post["link"],
            "user_id": post["user_id"],
            "username": post["username"],
            "user_url": post["user_url"],
            "is_live": post["is_live"],
            "was_live": post["was_live"],
            "factcheck": post["factcheck"],
            "shared_post_id": post["shared_post_id"],
            "shared_time": post["shared_time"],
            "shared_user_id": post["shared_user_id"],
            "reactions": post["reactions"],
            "reaction_count": post["reaction_count"],
            "with": post["with"],
            "page_id": post["page_id"],
        }, index=[postNum])
        
        print(post_df)
        postNum += 1
        df = pd.concat([df, post_df], axis=0)
    
    print(df)
    df.to_csv("raw_confessions.csv")
        
        
def transformData(csv_file_path: str):
    df = pd.read_csv(csv_file_path)
    new_df = pd.DataFrame()
    
    df = df.dropna(subset=["text", "time"])
    
    for index, row in df.iterrows():
        text = row["text"].split("\n")
        text = text[1:] # remove the first line as it is the confession ID
        text =  " ".join(text)
        date_time = row["time"]
        
        new_row = pd.DataFrame({"text": text, "time": date_time}, index=[index])
        new_df = pd.concat([new_df, new_row], axis=0)
       
    new_df.to_csv("processed_confessions.csv")
    

def loadData(csv_file):
    df = pd.read_csv(csv_file)   
    print(df["time"])
        
if __name__ == "__main__":
    # scrape(pages=2)
    # transformData("raw_confessions.csv")
    loadData("processed_confessions.csv")